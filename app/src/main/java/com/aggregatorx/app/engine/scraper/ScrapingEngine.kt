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
// Removed unresolved SmartNavigationEngine import
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import org.jsoup.Jsoup
import org.jsoup.nodes.Document
import com.aggregatorx.app.engine.util.EngineUtils
import java.net.URLEncoder
import java.util.concurrent.ConcurrentHashMap
import javax.inject.Inject
import javax.inject.Singleton

@Singleton
class ScrapingEngine @Inject constructor(
    private val providerDao: ProviderDao,
    private val scrapingConfigDao: ScrapingConfigDao,
    private val siteAnalysisDao: SiteAnalysisDao,
    // Removed unresolved SmartNavigationEngine from constructor
    private val smartContentClassifier: SmartContentClassifier,
    private val aiDecisionEngine: AIDecisionEngine,
    private val cloudflareBypassEngine: CloudflareBypassEngine,
    private val endpointDiscoveryEngine: EndpointDiscoveryEngine,
    private val nlpProcessor: NaturalLanguageQueryProcessor,
    private val webViewFetcher: WebViewFetcher
) {
    @Volatile private var currentProcessedQuery: ProcessedQuery? = null
    private val providerHealthMap = ConcurrentHashMap<String, ProviderHealth>()
    private val lastRequestTime   = ConcurrentHashMap<String, Long>()

    // Added clearCache method to resolve AggregatorRepository error
    fun clearCache() {
        providerHealthMap.clear()
        lastRequestTime.clear()
    }

    companion object {
        const val TARGET_RESULTS_PER_PROVIDER = 100 
        const val MIN_ACCEPTABLE_RESULTS      = 40  
        const val JSOUP_ESCALATION_THRESHOLD  = 8   
        const val MAX_PAGES                   = 15  

        private const val PAGE_TIMEOUT_MS          = 30_000L   
        private const val PER_PROVIDER_TIMEOUT_MS  = 180_000L  
        private const val DEFAULT_TIMEOUT          = 30_000
        private const val DEFAULT_RETRY_COUNT      = 3
        private const val DEFAULT_RETRY_DELAY      = 800L
        private const val DEFAULT_RATE_LIMIT_MS    = 50L
        private const val MAX_CONCURRENT_PROVIDERS = 20

        private val HARD_CATEGORY_URL_PATTERNS = listOf(
            "/genre/", "/genres/", "/category/", "/categories/", 
            "/browse/", "/filter/", "/filters/", "/tags/tag/",
            "/type/", "/sort/", "/order/", "?sort=", "?order=",
            "/all-", "/list/genre", "/movies/genre", "/series/genre",
            "?browse=", "?filter=", "?type=", "/listings/", "/catalog/",
            "/directory/", "/list/", "/browse-", "/section/", "/tag/",
            "/archive/", "/collection/", "/library/"
        )
        
        private val SUSPICIOUS_CATEGORY_NAMES = setOf(
            "action","comedy","drama","horror","thriller","romance",
            "sci-fi","science fiction","documentary","animation","anime",
            "sports","news","music","kids","family","adventure","fantasy",
            "crime","mystery","western","war","history","biography","adult"
        )
        
        private val CATEGORY_ITEM_KEYWORDS = setOf(
            "categories", "genres", "browse", "explore", "all titles",
            "view all", "show more", "more results", "latest", "trending",
            "popular", "featured", "new releases", "coming soon", "top",
            "best of", "recommendations", "curated", "collection"
        )
    }

    /**
     * Search all enabled providers concurrently. ALWAYS returns fresh data.
     */
    fun searchAllProviders(
        query: String,
        pages: Map<String, Int> = emptyMap()
    ): Flow<ProviderSearchResults> = flow {
        val processedQuery = nlpProcessor.processQuery(query)
        currentProcessedQuery = processedQuery

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
                                searchProviderWithGuaranteedResults(provider, query)
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
    }.flowOn(Dispatchers.IO)

    private suspend fun searchProviderWithGuaranteedResults(
        provider: Provider,
        query: String
    ): ProviderSearchResults = withContext(Dispatchers.IO) {
        val startTime = System.currentTimeMillis()
        val allResults = mutableListOf<SearchResult>()
        val seenUrls = mutableSetOf<String>()

        try {
            val page1Results = searchProviderWithJsoup(provider, query, page = 1)
            val filteredPage1 = filterQueryRelevantResults(page1Results, query)
            
            filteredPage1.forEach { 
                if (seenUrls.add(it.url)) allResults.add(it) 
            }

            if (filteredPage1.size < JSOUP_ESCALATION_THRESHOLD) {
                val webViewResults = tryWebViewSearch(provider, query)
                webViewResults.forEach { 
                    if (seenUrls.add(it.url)) allResults.add(it) 
                }
            } else {
                var currentPage = 2
                var consecutiveEmpty = 0
                
                while (allResults.size < MIN_ACCEPTABLE_RESULTS && 
                       currentPage <= MAX_PAGES && 
                       consecutiveEmpty < 3) {
                    
                    val pageResults = searchProviderWithJsoup(provider, query, currentPage)
                    val filtered = filterQueryRelevantResults(pageResults, query)
                    
                    if (filtered.isEmpty()) {
                        consecutiveEmpty++
                    } else {
                        consecutiveEmpty = 0
                        filtered.forEach { 
                            if (seenUrls.add(it.url)) allResults.add(it) 
                        }
                    }
                    currentPage++
                    delay(DEFAULT_RATE_LIMIT_MS)
                }

                if (allResults.size < MIN_ACCEPTABLE_RESULTS) {
                    val webViewResults = tryWebViewSearch(provider, query)
                    webViewResults.forEach { 
                        if (seenUrls.add(it.url)) allResults.add(it) 
                    }
                }
            }

            updateProviderHealth(provider, allResults.isNotEmpty(), System.currentTimeMillis() - startTime)

            return@withContext ProviderSearchResults(
                provider    = provider,
                results     = allResults.distinctBy { it.url }.sortedByDescending { it.relevanceScore },
                searchTime  = System.currentTimeMillis() - startTime,
                success     = allResults.isNotEmpty(),
                errorMessage = if (allResults.isEmpty()) "No results found" else null
            )

        } catch (e: Exception) {
            updateProviderHealth(provider, false, System.currentTimeMillis() - startTime)
            
            return@withContext ProviderSearchResults(
                provider    = provider,
                results     = emptyList(),
                searchTime  = System.currentTimeMillis() - startTime,
                success     = false,
                errorMessage = "Search failed: ${e.message?.take(80)}"
            )
        }
    }

    private suspend fun tryWebViewSearch(
        provider: Provider,
        query: String
    ): List<SearchResult> = withContext(Dispatchers.Main) {
        try {
            val searchUrl = buildSearchUrlForPage(provider, query, 1)
            var html = webViewFetcher.fetch(searchUrl, query, timeoutMs = 20_000L)
                ?: return@withContext emptyList()

            val allResults = mutableListOf<SearchResult>()
            val seenUrls = mutableSetOf<String>()

            var page = 0
            var consecutiveEmpty = 0

            while (allResults.size < TARGET_RESULTS_PER_PROVIDER && 
                   page < 8 && 
                   consecutiveEmpty < 3 &&
                   !html.isNullOrEmpty()) {
                
                val doc = Jsoup.parse(html!!, provider.baseUrl)
                val pageResults = parseResultsFromDocument(doc, provider, null)
                val filtered = filterQueryRelevantResults(pageResults, query)

                if (filtered.isEmpty()) {
                    consecutiveEmpty++
                } else {
                    consecutiveEmpty = 0
                    filtered.forEach {
                        if (seenUrls.add(it.url)) allResults.add(it)
                    }
                }

                html = tryGetNextPageHtml(html!!, provider, query, page) ?: break
                page++
                delay(500)
            }
            allResults
        } catch (e: Exception) {
            emptyList()
        }
    }

    private suspend fun tryGetNextPageHtml(
        currentHtml: String,
        provider: Provider,
        query: String,
        currentPage: Int
    ): String? = withContext(Dispatchers.Main) {
        try {
            val doc = Jsoup.parse(currentHtml)
            val nextButton = doc.selectFirst("a[rel='next'], a.next, button:contains(Next)")
            if (nextButton != null) {
                val href = nextButton.absUrl("href")
                if (href.isNotEmpty()) {
                    return@withContext webViewFetcher.fetch(href, query, timeoutMs = 15_000L)
                }
            }
            val nextUrl = buildSearchUrlForPage(provider, query, currentPage + 1)
            return@withContext webViewFetcher.fetch(nextUrl, query, timeoutMs = 15_000L)
        } catch (e: Exception) {
            null
        }
    }

    private suspend fun searchProviderWithJsoup(
        provider: Provider,
        query: String,
        page: Int = 1
    ): List<SearchResult> = withContext(Dispatchers.IO) {
        try {
            val searchUrl = buildSearchUrlForPage(provider, query, page)
            val doc = Jsoup.connect(searchUrl)
                .userAgent(EngineUtils.DEFAULT_USER_AGENT)
                .timeout(PAGE_TIMEOUT_MS.toInt())
                .get()

            return@withContext parseResultsFromDocument(doc, provider, null)
        } catch (e: Exception) {
            return@withContext emptyList()
        }
    }

    private fun buildSearchUrlForPage(provider: Provider, query: String, page: Int): String {
        val encodedQuery = URLEncoder.encode(query, "UTF-8")
        var searchUrl = provider.searchPattern
            .replace("{baseUrl}", provider.baseUrl)
            .replace("{query}", encodedQuery)

        if (page > 1) {
            searchUrl = when {
                searchUrl.contains("?") -> {
                    when {
                        searchUrl.contains("{page}") -> searchUrl.replace("{page}", page.toString())
                        else -> "$searchUrl&page=$page"
                    }
                }
                else -> {
                    when {
                        searchUrl.contains("{page}") -> searchUrl.replace("{page}", page.toString())
                        else -> "$searchUrl?page=$page"
                    }
                }
            }
        }
        return searchUrl
    }

    private suspend fun parseResultsFromDocument(
        doc: Document,
        provider: Provider,
        siteAnalysis: SiteAnalysis?
    ): List<SearchResult> = withContext(Dispatchers.Default) {
        val results = mutableListOf<SearchResult>()

        try {
            val elements = doc.select(
                "a[href]:has(~img), .result, .item, .card, article, " +
                "[class*='result-item'], [class*='search-result'], .torrent-row"
            ).take(50)

            for (element in elements) {
                try {
                    val title = element.selectFirst("a, h1, h2, h3, .title")?.text()?.trim() 
                        ?: continue
                    if (title.length < 3) continue
                    
                    val url = element.selectFirst("a")?.absUrl("href") ?: continue
                    if (url.isEmpty()) continue

                    val description = element.selectFirst("[class*='desc'], [class*='summary'], p")?.text() ?: ""
                    val thumbnailUrl = element.selectFirst("img")?.absUrl("src") ?: ""

                    results.add(
                        SearchResult(
                            title = title,
                            url = url,
                            description = description,
                            thumbnailUrl = thumbnailUrl,
                            providerName = provider.name,
                            providerId = provider.id,
                            quality = extractQuality(title + " " + description),
                            relevanceScore = 0.8f
                        )
                    )
                } catch (e: Exception) {
                    continue
                }
            }
        } catch (e: Exception) { }

        results
    }

    private fun filterQueryRelevantResults(
        results: List<SearchResult>,
        query: String
    ): List<SearchResult> {
        val queryTerms = query.lowercase().split(Regex("\\s+")).filter { it.length > 2 }

        return results.filter { result ->
            val titleLower = result.title.lowercase()
            val descLower = (result.description ?: "").lowercase()
            val urlLower = result.url.lowercase()

            val isHardCategory = HARD_CATEGORY_URL_PATTERNS.any { urlLower.contains(it) }
            if (isHardCategory) return@filter false

            val hasCategoryKeyword = CATEGORY_ITEM_KEYWORDS.any { urlLower.contains(it) }
            if (hasCategoryKeyword && result.description.isNullOrEmpty() && result.thumbnailUrl.isNullOrEmpty()) {
                return@filter false
            }

            queryTerms.any { titleLower.contains(it) || descLower.contains(it) }
        }
    }

    private fun extractQuality(text: String): String? {
        val upper = text.uppercase()
        return when {
            upper.contains("4K") -> "4K"
            upper.contains("1080") -> "1080p"
            upper.contains("720") -> "720p"
            else -> null
        }
    }

    private fun updateProviderHealth(
        provider: Provider,
        success: Boolean,
        time: Long
    ) {
        val h = providerHealthMap.getOrPut(provider.id) { ProviderHealth() }
        providerHealthMap[provider.id] = if (success)
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
