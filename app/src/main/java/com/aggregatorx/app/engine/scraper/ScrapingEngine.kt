package com.aggregatorx.app.engine.scraper

import com.aggregatorx.app.data.database.ProviderDao
import com.aggregatorx.app.data.database.ScrapingConfigDao
import com.aggregatorx.app.data.database.SiteAnalysisDao
import com.aggregatorx.app.data.model.*
import com.aggregatorx.app.engine.analyzer.SmartContentClassifier
import com.aggregatorx.app.engine.analyzer.EndpointDiscoveryEngine
import com.aggregatorx.app.engine.ai.AIDecisionEngine
import com.aggregatorx.app.engine.nlp.NaturalLanguageQueryProcessor
import com.aggregatorx.app.engine.nlp.ProcessedQuery
import com.aggregatorx.app.engine.network.CloudflareBypassEngine
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import org.jsoup.Jsoup
import org.jsoup.nodes.Document
import com.aggregatorx.app.engine.util.EngineUtils
import java.net.URLEncoder
// WebViewFetcher is in the same package — no import needed
import java.util.concurrent.ConcurrentHashMap
import javax.inject.Inject
import javax.inject.Singleton

/**
 * Multi-Provider Scraping Engine
 *
 * - Walks up to MAX_PAGES per provider until TARGET_RESULTS_PER_PROVIDER (100+) unique
 *   QUERY-RELEVANT results are collected — no UI pagination required.
 * - JS-heavy sites fall back to WebView when Jsoup yields < MIN_RESULTS_THRESHOLD.
 * - Per-page timeout: PAGE_TIMEOUT_MS. Total per-provider cap: PER_PROVIDER_TIMEOUT_MS (3 min).
 * - All providers run concurrently; one failure never stops the loop.
 * - Results stream to the UI as each provider completes (Loop 1).
 * - Loop 2 (smart/preference) is orchestrated by SearchViewModel after Loop 1 finishes.
 * 
 * SMART FILTERING:
 * - Hard blocks obvious catalog URLs (/genre/, /category/, /browse/ etc)
 * - Soft filters generic titles ONLY when missing description+thumbnail (query-related items kept)
 * - Enforces ALL results match query terms or concepts (no unrelated noise)
 */
@Singleton
class ScrapingEngine @Inject constructor(
    private val providerDao: ProviderDao,
    private val scrapingConfigDao: ScrapingConfigDao,
    private val siteAnalysisDao: SiteAnalysisDao,
    private val smartNavigationEngine: SmartNavigationEngine,
    private val smartContentClassifier: SmartContentClassifier,
    private val aiDecisionEngine: AIDecisionEngine,
    private val cloudflareBypassEngine: CloudflareBypassEngine,
    private val endpointDiscoveryEngine: EndpointDiscoveryEngine,
    private val nlpProcessor: NaturalLanguageQueryProcessor
) {
    @Volatile private var currentProcessedQuery: ProcessedQuery? = null
    private val providerHealthMap = ConcurrentHashMap<String, ProviderHealth>()
    private val lastRequestTime   = ConcurrentHashMap<String, Long>()

    companion object {
        // Result targets — increased for better coverage
        const val TARGET_RESULTS_PER_PROVIDER = 100  // aim for 90-100+ per provider
        const val MIN_RESULTS_THRESHOLD       = 8    // below this on page 1 → try WebView + more pages
        const val MAX_PAGES                   = 12   // max pages to walk per provider per query

        // Timeouts — extended to fetch more pages
        private const val PAGE_TIMEOUT_MS          = 30_000L   // per individual page fetch
        private const val PER_PROVIDER_TIMEOUT_MS  = 180_000L  // hard cap per provider (3 min covers multi-page)
        private const val DEFAULT_TIMEOUT          = 30_000
        private const val DEFAULT_RETRY_COUNT      = 3
        private const val DEFAULT_RETRY_DELAY      = 800L
        private const val DEFAULT_RATE_LIMIT_MS    = 50L
        private const val MAX_CONCURRENT_PROVIDERS = 20
        private const val CACHE_TTL_MS             = 10 * 60 * 1000L

        // ── HARD CATEGORY PATTERNS ─────────────────────────────────────────────────
        // These URLs are DEFINITELY catalog/filter pages, always block them
        private val HARD_CATEGORY_URL_PATTERNS = listOf(
            "/genre/", "/genres/", "/category/", "/categories/", 
            "/browse/", "/filter/", "/filters/", "/tags/tag/",
            "/type/", "/sort/", "/order/", "?sort=", "?order=",
            "/all-", "/list/genre", "/movies/genre", "/series/genre",
            "?browse=", "?filter=", "?type=", "/listings/", "/catalog/"
        )
        
        // ── SUSPICIOUS CATEGORY TITLES ────────────────────────────────────────────
        // These are red flags but only block if NO description AND NO thumbnail
        private val SUSPICIOUS_CATEGORY_NAMES = setOf(
            "action","comedy","drama","horror","thriller","romance",
            "sci-fi","science fiction","documentary","animation","anime",
            "sports","news","music","kids","family","adventure","fantasy",
            "crime","mystery","western","war","history","biography","adult"
        )
    }

    // ── Cache ─────────────────────────────────────────────────────────────────

    private data class CacheEntry(
        val results: List<ProviderSearchResults>,
        val timestamp: Long = System.currentTimeMillis()
    )

    private val resultCache = object : LinkedHashMap<String, CacheEntry>(100, 0.75f, true) {
        override fun removeEldestEntry(eldest: MutableMap.MutableEntry<String, CacheEntry>?) = size > 100
    }

    var cacheResults: Boolean = true
    fun clearCache() { synchronized(resultCache) { resultCache.clear() } }

    // ── Public API ─────────────────────────────────────────────────────────────

    /**
     * Search all enabled providers concurrently.
     * Each provider automatically walks pages until TARGET_RESULTS_PER_PROVIDER query-relevant
     * results are collected. Emits one [ProviderSearchResults] per provider as it completes.
     *
     * @param pages  Kept for API compatibility; pagination is now handled internally.
     */
    fun searchAllProviders(
        query: String,
        useCache: Boolean = cacheResults,
        pages: Map<String, Int> = emptyMap()
    ): Flow<ProviderSearchResults> = flow {
        val processedQuery = nlpProcessor.processQuery(query)
        currentProcessedQuery = processedQuery

        if (useCache) {
            val cached = synchronized(resultCache) { resultCache[query] }
            if (cached != null && System.currentTimeMillis() - cached.timestamp < CACHE_TTL_MS) {
                cached.results.forEach { emit(it) }
                return@flow
            }
        }

        var enabledProviders = providerDao.getEnabledProvidersSync()
        if (enabledProviders.isEmpty()) return@flow

        enabledProviders = enabledProviders.sortedWith(
            compareByDescending<Provider> { it.successRate }.thenBy { it.avgResponseTime }
        )

        val semaphore = Semaphore(MAX_CONCURRENT_PROVIDERS)
        val results   = mutableListOf<ProviderSearchResults>()

        coroutineScope {
            val deferred = enabledProviders.map { provider ->
                async {
                    semaphore.withPermit {
                        try {
                            withTimeoutOrNull(PER_PROVIDER_TIMEOUT_MS) {
                                safeSearchProvider(provider, query)
                            } ?: ProviderSearchResults(
                                provider     = provider,
                                results      = emptyList(),
                                searchTime   = PER_PROVIDER_TIMEOUT_MS,
                                success      = false,
                                errorMessage = "Provider timed out after ${PER_PROVIDER_TIMEOUT_MS / 1000}s"
                            )
                        } catch (e: CancellationException) { throw e }
                        catch (e: Exception) {
                            ProviderSearchResults(provider, emptyList(), 0L, false,
                                "Error: ${e.message?.take(100)}")
                        }
                    }
                }
            }

            deferred.forEachIndexed { idx, d ->
                val provider = enabledProviders.getOrNull(idx)
                try {
                    val r = d.await()
                    results.add(r)
                    emit(r)
                } catch (e: CancellationException) {
                    provider?.let { p ->
                        val r = ProviderSearchResults(p, emptyList(), 0L, false, "Cancelled")
                        results.add(r); emit(r)
                    }
                } catch (e: Exception) {
                    provider?.let { p ->
                        val r = ProviderSearchResults(p, emptyList(), 0L, false, "Internal Error")
                        results.add(r); emit(r)
                    }
                }
            }
        }

        if (useCache && results.any { it.success }) {
            synchronized(resultCache) { resultCache[query] = CacheEntry(results) }
        }
    }.flowOn(Dispatchers.IO)

    // ── Per-provider orchestration ────────────────────────────────────────────

    private suspend fun safeSearchProvider(provider: Provider, query: String): ProviderSearchResults {
        val startTime = System.currentTimeMillis()
        val domain    = extractDomain(provider.baseUrl)
        val isCooling = provider.failedSearches > 5 &&
            (provider.failedSearches.toFloat() / maxOf(provider.totalSearches, 1)) > 0.7f

        return try {
            val result = if (isCooling) {
                tryFallbackScraping(provider, query, startTime, Exception("Cooldown"))
            } else {
                fetchUntilTarget(provider, query, startTime)
            }

            if (result.success && result.results.isNotEmpty()) {
                val validated = validateAndFilterResults(result.results, query)
                if (validated.isEmpty()) {
                    aiDecisionEngine.learnFromFailure(domain, "CATEGORY_PAGE", "Invalid results",
                        ScrapingStrategy.HTML_PARSING, null, provider.baseUrl)
                    retryWithNlpQueries(provider, query, startTime) ?: result.copy(
                        results = emptyList(), success = false,
                        errorMessage = "Category results filtered"
                    )
                } else {
                    aiDecisionEngine.learnFromSuccess(domain, ScrapingStrategy.HTML_PARSING,
                        null, null, null, validated.size, System.currentTimeMillis() - startTime)
                    result.copy(results = validated)
                }
            } else {
                retryWithNlpQueries(provider, query, startTime) ?: result
            }
        } catch (e: CancellationException) { throw e }
        catch (e: Exception) {
            aiDecisionEngine.learnFromFailure(domain, "EXCEPTION", e.message,
                ScrapingStrategy.HTML_PARSING, null, provider.baseUrl)
            val fallback = tryFallbackScraping(provider, query, startTime, e)
            if (fallback.success && fallback.results.isNotEmpty()) {
                val validated = validateAndFilterResults(fallback.results, query)
                if (validated.isNotEmpty()) return fallback.copy(results = validated)
            }
            retryWithNlpQueries(provider, query, startTime) ?: fallback
        }
    }

    /**
     * Core multi-page fetch loop.
     *
     * Walks pages 0..MAX_PAGES-1 until TARGET_RESULTS_PER_PROVIDER QUERY-RELEVANT results are
     * collected or the provider runs out of pages. Each page has its own PAGE_TIMEOUT_MS
     * deadline; the whole provider is capped by PER_PROVIDER_TIMEOUT_MS in the caller.
     *
     * When page 0 returns fewer than MIN_RESULTS_THRESHOLD results, a WebView fetch is
     * attempted in parallel before continuing to page 1.
     */
    private suspend fun fetchUntilTarget(
        provider: Provider,
        query: String,
        startTime: Long
    ): ProviderSearchResults {
        val allResults = mutableListOf<SearchResult>()
        val seenUrls   = mutableSetOf<String>()

        val processed      = currentProcessedQuery
        val effectiveQuery = if (processed != null && processed.isNaturalLanguage)
            processed.searchQueries.firstOrNull() ?: query else query

        // Discover search URL once (reused across pages)
        val baseSearchUrl = try {
            withTimeoutOrNull(10_000L) {
                smartNavigationEngine.findSearchUrl(provider.baseUrl, effectiveQuery)
            }
        } catch (_: Exception) { null }

        for (page in 0 until MAX_PAGES) {
            if (allResults.size >= TARGET_RESULTS_PER_PROVIDER) break

            try {
                val pageResults = withTimeoutOrNull(PAGE_TIMEOUT_MS) {
                    fetchSinglePage(provider, effectiveQuery, query, baseSearchUrl, page)
                } ?: emptyList()

                pageResults.forEach { r -> if (seenUrls.add(r.url)) allResults.add(r) }

                // Page 0 low-yield → try WebView in parallel
                if (page == 0 && allResults.size < MIN_RESULTS_THRESHOLD) {
                    val wvResults = withTimeoutOrNull(20_000L) {
                        tryWebViewFetch(provider, effectiveQuery, query)
                    } ?: emptyList()
                    wvResults.forEach { r -> if (seenUrls.add(r.url)) allResults.add(r) }
                }

                // End of results for this provider
                if (pageResults.isEmpty()) break

            } catch (e: CancellationException) { throw e }
            catch (_: Exception) {
                if (page == 0) break   // first page failed hard — give up
                // subsequent page failure → just stop paging
                break
            }
        }

        updateProviderHealth(provider.id, allResults.isNotEmpty(), System.currentTimeMillis() - startTime)
        return ProviderSearchResults(
            provider   = provider,
            results    = allResults.sortedByDescending { it.relevanceScore },
            searchTime = System.currentTimeMillis() - startTime,
            success    = allResults.isNotEmpty(),
            hasMore    = false   // pagination fully internal; UI shows all results at once
        )
    }

    private suspend fun fetchSinglePage(
        provider: Provider,
        effectiveQuery: String,
        originalQuery: String,
        baseSearchUrl: String?,
        page: Int
    ): List<SearchResult> {
        val searchUrl = when {
            baseSearchUrl != null -> buildPagedUrl(baseSearchUrl, page)
            else                  -> buildFallbackSearchUrl(provider, effectiveQuery, page)
        }
        val document = fetchDocument(searchUrl)
        val (_, activeDoc) = if (smartNavigationEngine.isCategoryPage(searchUrl, document)) {
            smartNavigationEngine.navigatePastCategory(provider.baseUrl, document, effectiveQuery)
                ?: (searchUrl to document)
        } else searchUrl to document

        return extractResultsWithThumbnails(activeDoc, provider, originalQuery)
    }

    /** WebView fetch for JS-rendered pages — runs on Main thread as required by WebView. */
    private suspend fun tryWebViewFetch(
        provider: Provider,
        effectiveQuery: String,
        originalQuery: String
    ): List<SearchResult> = withContext(Dispatchers.Main) {
        try {
            val searchUrl = smartNavigationEngine.findSearchUrl(provider.baseUrl, effectiveQuery)
                ?: buildFallbackSearchUrl(provider, effectiveQuery, 0)
            val html = WebViewFetcher.fetch(searchUrl, effectiveQuery, timeoutMs = 18_000L)
            if (html.isNullOrBlank()) return@withContext emptyList()
            val doc = Jsoup.parse(html, provider.baseUrl)
            extractResultsWithThumbnails(doc, provider, originalQuery)
        } catch (_: Exception) { emptyList() }
    }

    private fun buildPagedUrl(baseUrl: String, page: Int): String {
        if (page == 0) return baseUrl
        return when {
            baseUrl.contains("page=")  -> baseUrl.replace(Regex("page=\\d+"), "page=${page + 1}")
            baseUrl.contains("?")      -> "$baseUrl&page=${page + 1}"
            baseUrl.endsWith("/")      -> "${baseUrl}page/${page + 1}"
            else                       -> "$baseUrl/page/${page + 1}"
        }
    }

    private fun buildFallbackSearchUrl(provider: Provider, query: String, page: Int): String {
        val encoded = URLEncoder.encode(query, "UTF-8")
        val base = provider.searchPattern
            .replace("{query}", encoded)
            .replace("{QUERY}", encoded)
            .replace("{baseUrl}", provider.baseUrl.trimEnd('/'))
            .let { if (it.startsWith("http")) it else "${provider.baseUrl.trimEnd('/')}/$it" }
        return if (page > 0) buildPagedUrl(base, page) else base
    }

    // ── NLP variant retry ─────────────────────────────────────────────────────

    private suspend fun retryWithNlpQueries(
        provider: Provider,
        originalQuery: String,
        startTime: Long
    ): ProviderSearchResults? {
        val processed = currentProcessedQuery ?: return null
        val variants  = processed.searchQueries
            .filter { it.lowercase() != originalQuery.lowercase() }.take(3)
        if (variants.isEmpty()) return null

        val allResults = mutableListOf<SearchResult>()
        val seenUrls   = mutableSetOf<String>()

        for (variant in variants) {
            try {
                val r = fetchUntilTarget(provider, variant, startTime)
                if (r.success) {
                    validateAndFilterResults(r.results, originalQuery)
                        .forEach { if (seenUrls.add(it.url)) allResults.add(it) }
                }
                if (allResults.size >= TARGET_RESULTS_PER_PROVIDER) break
            } catch (_: Exception) { continue }
        }

        return if (allResults.isNotEmpty())
            ProviderSearchResults(provider, allResults.sortedByDescending { it.relevanceScore },
                System.currentTimeMillis() - startTime, true)
        else null
    }

    // ── Fallback ─────────────────────────────────────────────────────────────

    private suspend fun tryFallbackScraping(
        provider: Provider,
        query: String,
        startTime: Long,
        cause: Exception
    ): ProviderSearchResults = try {
        searchProvider(provider, query, 0)
    } catch (e: Exception) {
        ProviderSearchResults(provider, emptyList(), System.currentTimeMillis() - startTime,
            false, "Fallback failed: ${e.message?.take(80)}")
    }

    // ── Result extraction ─────────────────────────────────────────────────────

    private fun extractResultsWithThumbnails(
        document: Document,
        provider: Provider,
        query: String
    ): List<SearchResult> {
        val results      = mutableListOf<SearchResult>()
        val contentLinks = smartNavigationEngine.extractContentLinks(document, provider.baseUrl)

        for (link in contentLinks) {
            val title = link.title.takeIf { it.length > 2 }
                ?: extractTitleFromUrl(link.url) ?: continue
            val result = SearchResult(
                title          = title,
                url            = link.url,
                thumbnailUrl   = link.thumbnail,
                description    = findDescriptionInDocument(document, link.url),
                providerId     = provider.id,
                providerName   = provider.name,
                relevanceScore = calculateRelevanceScore(title, query, null, link.url)
            )
            if (matchesQueryEnhanced(result, query)) results.add(result)
        }

        return if (results.size < 5) results + extractResultsGeneric(document, provider, query)
        else results
    }

    private fun extractResultsGeneric(
        document: Document,
        provider: Provider,
        query: String
    ): List<SearchResult> {
        val results  = mutableListOf<SearchResult>()
        val elements = document.select(
            "article, .item, .result, .card, .post, li:has(a), tr:has(a), " +
            "[class*='item']:has(a), [class*='result']:has(a), [class*='card']:has(a)"
        ).take(300)  // Increased from 200 to 300 to capture more candidates

        for (el in elements) {
            try {
                val anchor = el.selectFirst("a[href]") ?: continue
                val href   = anchor.absUrl("href")
                if (href.isEmpty()) continue
                val title  = anchor.text().takeIf { it.length > 2 }
                    ?: el.selectFirst("h1,h2,h3,h4,.title,.name")?.text()
                    ?: extractTitleFromUrl(href) ?: continue
                val thumb  = el.selectFirst("img[src]")?.absUrl("src")
                val desc   = el.selectFirst("p,.description,.summary,.excerpt")?.text()
                val result = SearchResult(
                    title          = title,
                    url            = href,
                    thumbnailUrl   = thumb,
                    description    = desc,
                    providerId     = provider.id,
                    providerName   = provider.name,
                    relevanceScore = calculateRelevanceScore(title, query, desc, href)
                )
                if (matchesQueryEnhanced(result, query)) results.add(result)
            } catch (_: Exception) {}
        }
        return results.distinctBy { it.url }
    }

    // ── Document fetching ─────────────────────────────────────────────────────

    private suspend fun fetchDocument(url: String, config: ScrapingConfig? = null): Document =
        withContext(Dispatchers.IO) {
            var lastEx: Exception? = null
            val timeout = config?.timeout ?: DEFAULT_TIMEOUT

            repeat(DEFAULT_RETRY_COUNT) { attempt ->
                try {
                    val conn = Jsoup.connect(url)
                        .userAgent(getRandomUserAgent())
                        .timeout(timeout)
                        .followRedirects(true)
                    val resp = conn.execute()
                    val doc  = resp.parse()
                    if (resp.statusCode() in 200..299 && !doc.html().contains("cf_chl_opt"))
                        return@withContext doc
                } catch (e: Exception) {
                    lastEx = e
                    delay(DEFAULT_RETRY_DELAY * (attempt + 1))
                }
            }

            cloudflareBypassEngine.fetchJsoupDocument(url, timeout)
                ?.let { return@withContext it }

            val headless = HeadlessBrowserHelper.fetchPageContentWithShadowAndAdSkip(url, null, timeout)
            if (!headless.isNullOrEmpty()) return@withContext Jsoup.parse(headless, url)

            throw lastEx ?: Exception("Fetch failed for $url")
        }

    // ── Validation & scoring ──────────────────────────────────────────────────
    
    /**
     * Smart filtering that ALWAYS enforces query relevance:
     * 1. Hard-block obvious catalog URLs
     * 2. Soft-block generic titles only if NO description/thumbnail
     * 3. REQUIRE all results match query keywords, concepts, or semantic relevance
     * 
     * Result: 100+ results per provider, all matching your search query
     */
    private fun validateAndFilterResults(
        results: List<SearchResult>,
        query: String
    ): List<SearchResult> {
        val queryWords = query.lowercase().split(Regex("\\s+")).filter { it.length > 2 }
        val processed  = currentProcessedQuery

        return results.filter { result ->
            val titleLower = result.title.lowercase()
            val urlLower   = result.url.lowercase()

            // ════════════════════════════════════════════════════════════════════
            // BLOCK 1: HARD CATALOG PATTERNS — Always remove these
            // ════════════════════════════════════════════════════════════════════
            if (result.title.length < 3) return@filter false
            if (HARD_CATEGORY_URL_PATTERNS.any { urlLower.contains(it) }) return@filter false

            // ════════════════════════════════════════════════════════════════════
            // BLOCK 2: SOFT CATEGORY TITLES — Only block if NO metadata
            // ════════════════════════════════════════════════════════════════════
            // If title is a genre/category name BUT has description/thumbnail, keep it
            // (it's a real content page like "Best Action Movies" or has metadata)
            if (titleLower.trim() in SUSPICIOUS_CATEGORY_NAMES) {
                val hasDescription = !result.description.isNullOrBlank()
                val hasThumbnail = !result.thumbnailUrl.isNullOrBlank()
                if (!hasDescription && !hasThumbnail) {
                    return@filter false  // Likely a bare category page
                }
                // Has metadata → it's a real result, keep it
            }

            // ════════════════════════════════════════════════════════════════════
            // BLOCK 3: QUERY RELEVANCE — MUST pass at least one relevance check
            // ════════════════════════════════════════════════════════════════════
            val combined      = "$titleLower ${result.description?.lowercase() ?: ""} $urlLower"
            val hasKeyword    = queryWords.any { combined.contains(it) }
            val hasConcept    = processed?.conceptTerms?.any { combined.contains(it) } ?: false
            val semanticScore = processed?.let {
                nlpProcessor.calculateSemanticRelevance(result.title, result.description, it.concepts)
            } ?: 0f

            // ENFORCE: Result must be query-related (one of these must be true)
            hasKeyword || hasConcept || semanticScore >= 12f
        }
    }

    private fun calculateRelevanceScore(
        title: String,
        query: String,
        description: String? = null,
        url: String? = null
    ): Float {
        val titleLower = title.lowercase()
        val queryLower = query.lowercase()
        val terms      = queryLower.split(Regex("\\s+")).filter { it.length > 1 }
        if (terms.isEmpty()) return 0f

        var score = 0f
        if (titleLower.contains(queryLower)) score += 50f
        terms.forEach { term ->
            if (titleLower.contains(term)) {
                score += 10f
                if (titleLower.startsWith(term)) score += 5f
            }
        }
        currentProcessedQuery?.let {
            score += nlpProcessor.calculateSemanticRelevance(title, description, it.concepts)
        }
        return score
    }

    /**
     * STRICT: Result MUST match query to be included.
     * Uses keyword matching, NLP concepts, and semantic similarity.
     */
    private fun matchesQueryEnhanced(result: SearchResult, query: String): Boolean {
        val terms    = query.lowercase().split(Regex("\\s+")).filter { it.length > 2 }
        val combined = "${result.title} ${result.description ?: ""} ${result.url}".lowercase()
        
        // Check for direct keyword match
        if (terms.any { combined.contains(it) }) return true
        
        val processed = currentProcessedQuery ?: return false
        
        // Check for NLP concept match
        if (processed.conceptTerms.any { combined.contains(it) }) return true
        
        // Check for semantic relevance
        val semanticScore = nlpProcessor.calculateSemanticRelevance(
            result.title, result.description, processed.concepts)
        return semanticScore >= 12f  // Threshold for semantic match
    }

    // ── Compat shim (used by other engines) ──────────────────────────────────

    suspend fun searchProviderSmart(
        provider: Provider,
        query: String,
        pageNum: Int = 0
    ): ProviderSearchResults = fetchUntilTarget(provider, query, System.currentTimeMillis())

    private suspend fun searchProvider(
        provider: Provider,
        query: String,
        pageNum: Int = 0
    ): ProviderSearchResults {
        val startTime = System.currentTimeMillis()
        val searchUrl = buildFallbackSearchUrl(provider, query, pageNum)
        return try {
            enforceRateLimit(provider.id)
            providerDao.incrementSearchCount(provider.id)
            val doc     = fetchDocument(searchUrl)
            val results = extractResultsWithThumbnails(doc, provider, query)
            val valid   = validateAndFilterResults(results, query)
            updateProviderHealth(provider.id, valid.isNotEmpty(), System.currentTimeMillis() - startTime)
            ProviderSearchResults(provider, valid, System.currentTimeMillis() - startTime, valid.isNotEmpty())
        } catch (e: Exception) {
            updateProviderHealth(provider.id, false, System.currentTimeMillis() - startTime)
            ProviderSearchResults(provider, emptyList(), System.currentTimeMillis() - startTime,
                false, e.message?.take(100))
        }
    }

    // ── Utilities ──────────────────────────────────────────────────────────────

    private suspend fun enforceRateLimit(providerId: String) {
        val last = lastRequestTime[providerId] ?: 0L
        val wait = DEFAULT_RATE_LIMIT_MS - (System.currentTimeMillis() - last)
        if (wait > 0) delay(wait)
        lastRequestTime[providerId] = System.currentTimeMillis()
    }

    private fun extractDomain(url: String): String = EngineUtils.extractDomain(url)

    private fun extractTitleFromUrl(url: String): String? {
        return try {
            val path = java.net.URI(url).path ?: return null
            val last = path.trimEnd('/').substringAfterLast('/')
            if (last.length < 3) return null
            last.replace(Regex("[-_]"), " ")
                .replace(Regex("\\.[a-z]{2,4}$"), "")
                .split(" ")
                .joinToString(" ") { it.replaceFirstChar { c -> c.uppercase() } }
                .takeIf { it.length > 2 }
        } catch (_: Exception) { null }
    }

    private fun findDescriptionInDocument(document: Document, url: String): String? =
        document.select("meta[name=description]").firstOrNull()?.attr("content")
            ?: document.select(".description,.summary,.excerpt,p").firstOrNull()?.text()?.take(200)

    private fun getRandomUserAgent(): String = EngineUtils.USER_AGENTS.random()

    private fun updateProviderHealth(id: String, success: Boolean, time: Long) {
        val h = providerHealthMap.getOrPut(id) { ProviderHealth() }
        providerHealthMap[id] = if (success)
            h.copy(successCount = h.successCount + 1, avgResponseTime = (h.avgResponseTime + time) / 2)
        else
            h.copy(failureCount = h.failureCount + 1)
    }

    data class ProviderHealth(
        val successCount: Int = 0,
        val failureCount: Int = 0,
        val avgResponseTime: Long = 0
    )
}
