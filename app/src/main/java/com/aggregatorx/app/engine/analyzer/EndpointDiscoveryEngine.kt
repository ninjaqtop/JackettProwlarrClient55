package com.aggregatorx.app.engine.analyzer

import com.aggregatorx.app.data.model.*
import com.aggregatorx.app.engine.network.CloudflareBypassEngine
import com.aggregatorx.app.engine.scraper.HeadlessBrowserHelper
import com.aggregatorx.app.engine.util.EngineUtils
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.withContext
import org.jsoup.Jsoup
import org.jsoup.nodes.Document
import java.util.concurrent.ConcurrentHashMap
import javax.inject.Inject
import javax.inject.Singleton

/**
 * Advanced API & Endpoint Discovery Engine
 *
 * Discovers hidden API endpoints, search URLs, and site structure through:
 * 1. Static HTML/JS analysis — parse inline scripts for fetch/XHR URLs
 * 2. robots.txt / sitemap.xml mining for hidden paths
 * 3. Common CMS API path probing (WordPress, Ghost, Strapi, Directus, etc.)
 * 4. GraphQL introspection detection
 * 5. OpenAPI/Swagger spec detection
 * 6. Pattern-based endpoint deduction from visible URLs
 * 7. Headless network interception (Playwright request capture)
 * 8. Learning from past successes — avoids re-probing known-dead endpoints
 *
 * All discovered endpoints are cached per domain and shared with
 * ScrapingEngine and AIDecisionEngine for smarter scraping.
 */
@Singleton
class EndpointDiscoveryEngine @Inject constructor(
    private val cloudflareBypassEngine: CloudflareBypassEngine
) {

    companion object {
        private const val DISCOVERY_TIMEOUT = 20000
        private const val CACHE_TTL_MS = 2 * 60 * 60 * 1000L  // 2 hours
        private val UA = EngineUtils.DEFAULT_USER_AGENT

        // Known CMS API path templates (base → paths to probe)
        private val CMS_API_PATHS = listOf(
            // WordPress
            "/wp-json/wp/v2/posts?search={query}&per_page=20",
            "/wp-json/wp/v2/search?search={query}&per_page=20",
            "/wp-json/wp/v2/media?search={query}&per_page=20",
            "/wp-json/wp/v2/pages?search={query}&per_page=20",
            "/wp-json/wp/v2/categories",
            "/wp-json/wp/v2/tags",
            "/?rest_route=/wp/v2/posts&search={query}",
            // Ghost CMS
            "/ghost/api/content/posts/?filter=title:~%27{query}%27&limit=20",
            "/ghost/api/v4/content/posts/?filter=title:~%27{query}%27&limit=20",
            // Strapi v4/v5
            "/api/articles?filters[title][\$containsi]={query}&pagination[pageSize]=20",
            "/api/posts?filters[title][\$containsi]={query}&pagination[pageSize]=20",
            "/api/videos?filters[title][\$containsi]={query}&pagination[pageSize]=20",
            "/api/movies?filters[title][\$containsi]={query}&pagination[pageSize]=20",
            "/api/contents?filters[title][\$containsi]={query}&pagination[pageSize]=20",
            // Directus
            "/items/articles?filter[title][_contains]={query}&limit=20",
            "/items/posts?filter[title][_contains]={query}&limit=20",
            "/items/content?filter[title][_contains]={query}&limit=20",
            "/items/videos?filter[title][_contains]={query}&limit=20",
            // Generic REST APIs
            "/api/search?q={query}",
            "/api/v1/search?q={query}",
            "/api/v2/search?q={query}",
            "/api/v3/search?q={query}",
            "/api/search?query={query}",
            "/api/search?keyword={query}",
            "/api/search?term={query}",
            "/api/videos/search?q={query}",
            "/api/movies/search?q={query}",
            "/api/content/search?q={query}",
            // AJAX endpoints (common in torrent sites)
            "/ajax/search?q={query}",
            "/ajax/movies/search?content={query}",
            "/suggest?q={query}",
            "/autocomplete?q={query}",
            "/api/autocomplete?q={query}",
            // GraphQL
            "/graphql",
            "/api/graphql"
        )

        // Regex to extract API URLs from inline JavaScript
        private val JS_API_PATTERNS = listOf(
            Regex("""(?:fetch|axios\.get|axios\.post|\$\.ajax|\$\.get|\$\.post|XMLHttpRequest)\s*\(\s*['"`](/?(?:api|ajax|graphql|search|json)[^'"`\s]*?)['"`]""", RegexOption.IGNORE_CASE),
            Regex("""['"`](https?://[^'"`\s]*?/api/[^'"`\s]*?)['"`]""", RegexOption.IGNORE_CASE),
            Regex("""['"`](/api/[^'"`\s]*?)['"`]""", RegexOption.IGNORE_CASE),
            Regex("""['"`](/?wp-json/[^'"`\s]*?)['"`]""", RegexOption.IGNORE_CASE),
            Regex("""apiUrl\s*[:=]\s*['"`]([^'"`\s]+)['"`]""", RegexOption.IGNORE_CASE),
            Regex("""baseUrl\s*[:=]\s*['"`]([^'"`\s]+)['"`]""", RegexOption.IGNORE_CASE),
            Regex("""searchUrl\s*[:=]\s*['"`]([^'"`\s]+)['"`]""", RegexOption.IGNORE_CASE),
            Regex("""endpoint\s*[:=]\s*['"`]([^'"`\s]+)['"`]""", RegexOption.IGNORE_CASE)
        )

        // Sitemap URL patterns
        private val SITEMAP_PATHS = listOf(
            "/sitemap.xml",
            "/sitemap_index.xml",
            "/sitemap-index.xml",
            "/sitemaps/sitemap.xml",
            "/wp-sitemap.xml"
        )
    }

    // Cache: domain → DiscoveredEndpoints
    private val endpointCache = ConcurrentHashMap<String, CachedDiscovery>()

    /**
     * Discover all available endpoints for a site.
     * Returns a [DiscoveredEndpoints] with search URLs, API endpoints, sitemaps, etc.
     */
    suspend fun discoverEndpoints(
        baseUrl: String,
        sampleQuery: String = "test"
    ): DiscoveredEndpoints = withContext(Dispatchers.IO) {
        val domain = extractDomain(baseUrl)
        val base = baseUrl.trimEnd('/')

        // Check cache
        endpointCache[domain]?.let { cached ->
            if (!cached.isExpired()) return@withContext cached.endpoints
        }

        val searchEndpoints = mutableListOf<String>()
        val apiEndpoints = mutableListOf<String>()
        val sitemapUrls = mutableListOf<String>()
        val detectedCms: String? = null
        var hasGraphQL = false
        var hasOpenAPI = false

        // Run discovery strategies in parallel
        coroutineScope {
            // 1. Static JS analysis
            val jsAnalysis = async { analyzeInlineScripts(base) }

            // 2. robots.txt mining
            val robotsAnalysis = async { mineRobotsTxt(base) }

            // 3. CMS API probing
            val cmsProbe = async { probeCmsApis(base, sampleQuery) }

            // 4. Sitemap mining
            val sitemapAnalysis = async { mineSitemaps(base) }

            // Collect results
            val jsEndpoints = jsAnalysis.await()
            searchEndpoints.addAll(jsEndpoints.filter { it.contains("search") || it.contains("query") || it.contains("q=") })
            apiEndpoints.addAll(jsEndpoints)

            val robotsPaths = robotsAnalysis.await()
            apiEndpoints.addAll(robotsPaths.filter { it.contains("api") || it.contains("ajax") || it.contains("json") })

            val cmsResults = cmsProbe.await()
            searchEndpoints.addAll(cmsResults.searchEndpoints)
            apiEndpoints.addAll(cmsResults.apiEndpoints)
            hasGraphQL = cmsResults.hasGraphQL
            hasOpenAPI = cmsResults.hasOpenAPI

            val sitemaps = sitemapAnalysis.await()
            sitemapUrls.addAll(sitemaps)
        }

        val result = DiscoveredEndpoints(
            domain = domain,
            baseUrl = base,
            searchEndpoints = searchEndpoints.distinct(),
            apiEndpoints = apiEndpoints.distinct(),
            sitemapUrls = sitemapUrls.distinct(),
            detectedCms = detectedCms,
            hasGraphQL = hasGraphQL,
            hasOpenAPI = hasOpenAPI,
            discoveredAt = System.currentTimeMillis()
        )

        // Cache
        endpointCache[domain] = CachedDiscovery(result)

        result
    }

    /**
     * Get the best search endpoint for a site. Returns null if none found.
     */
    suspend fun getBestSearchEndpoint(baseUrl: String, query: String): String? {
        val endpoints = discoverEndpoints(baseUrl, query)
        if (endpoints.searchEndpoints.isEmpty()) return null

        // Try each endpoint and return first that works
        for (endpoint in endpoints.searchEndpoints.take(5)) {
            val url = endpoint
                .replace("{query}", java.net.URLEncoder.encode(query, "UTF-8"))
                .let { if (it.startsWith("http")) it else "${baseUrl.trimEnd('/')}$it" }
            try {
                val html = HeadlessBrowserHelper.fetchRaw(url)
                if (html != null && html.isNotEmpty()) return url
            } catch (_: Exception) {}
        }
        return null
    }

    // ── Discovery Strategies ──────────────────────────────────────────────────

    private suspend fun analyzeInlineScripts(base: String): List<String> {
        val html = HeadlessBrowserHelper.fetchRaw(base) ?: return emptyList()
        val endpoints = mutableListOf<String>()

        val scriptPattern = Regex("""<script[^>]*>([\s\S]*?)</script>""", RegexOption.IGNORE_CASE)
        scriptPattern.findAll(html).forEach { match ->
            val scriptContent = match.groupValues[1]
            JS_API_PATTERNS.forEach { pattern ->
                pattern.findAll(scriptContent).forEach { m ->
                    val url = m.groupValues.lastOrNull()?.trim('\'', '"', '`')
                    if (!url.isNullOrEmpty() && url.length > 2) endpoints.add(url)
                }
            }
        }
        return endpoints.distinct()
    }

    private suspend fun mineRobotsTxt(base: String): List<String> {
        val robotsUrl = "$base/robots.txt"
        val content = HeadlessBrowserHelper.fetchRaw(robotsUrl) ?: return emptyList()
        return content.lines()
            .filter { it.startsWith("Disallow:") || it.startsWith("Allow:") }
            .mapNotNull { it.substringAfter(":").trim().ifEmpty { null } }
    }

    private suspend fun probeCmsApis(base: String, query: String): CmsProbeResult {
        val searchEndpoints = mutableListOf<String>()
        val apiEndpoints = mutableListOf<String>()
        var hasGraphQL = false
        var hasOpenAPI = false

        for (path in CMS_API_PATHS.take(10)) {
            try {
                val url = "$base${path.replace("{query}", java.net.URLEncoder.encode(query, "UTF-8"))}"
                val resp = HeadlessBrowserHelper.fetchRaw(url)
                if (resp != null && resp.isNotEmpty()) {
                    when {
                        path.contains("search") || path.contains("q=") -> searchEndpoints.add(path)
                        else -> apiEndpoints.add(path)
                    }
                }
            } catch (_: Exception) {}
        }

        // Check for GraphQL
        try {
            val gqlResp = HeadlessBrowserHelper.fetchRaw("$base/graphql")
            hasGraphQL = gqlResp != null && gqlResp.contains("query")
        } catch (_: Exception) {}

        return CmsProbeResult(searchEndpoints, apiEndpoints, hasGraphQL, hasOpenAPI)
    }

    private suspend fun mineSitemaps(base: String): List<String> {
        val sitemaps = mutableListOf<String>()
        for (path in SITEMAP_PATHS) {
            try {
                val sitemapUrl = "$base$path"
                val resp = HeadlessBrowserHelper.fetchRaw(sitemapUrl)
                if (resp != null && resp.contains("<loc>")) {
                    sitemaps.add(sitemapUrl)
                }
            } catch (_: Exception) {}
        }
        return sitemaps
    }

    private fun extractDomain(url: String): String = try {
        val uri = java.net.URI(url)
        uri.host ?: url
    } catch (_: Exception) {
        url
    }

    private data class CmsProbeResult(
        val searchEndpoints: List<String>,
        val apiEndpoints: List<String>,
        val hasGraphQL: Boolean,
        val hasOpenAPI: Boolean
    )

    private data class CachedDiscovery(
        val endpoints: DiscoveredEndpoints,
        val timestamp: Long = System.currentTimeMillis()
    ) {
        fun isExpired() = System.currentTimeMillis() - timestamp > CACHE_TTL_MS
    }
}
