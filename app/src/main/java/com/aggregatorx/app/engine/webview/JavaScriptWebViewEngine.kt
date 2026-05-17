package com.aggregatorx.app.engine.webview

import android.webkit.WebResourceRequest
import android.webkit.WebResourceResponse
import android.webkit.WebView
import android.webkit.WebViewClient
import kotlinx.coroutines.CompletableFuture
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlinx.coroutines.withTimeout
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException

/**
 * WebView Engine for JavaScript-heavy providers.
 *
 * Handles providers that require:
 * - JavaScript execution to render search results
 * - Dynamic content loading (AJAX, fetch calls)
 * - Client-side pagination or infinite scroll
 * - Complex DOM manipulation
 *
 * Returns rendered HTML after JS execution completes.
 */
class JavaScriptWebViewEngine(private val webView: WebView) {

    private var isPageReady = false
    private var pageContent = ""
    private val readySignal = CompletableFuture<String>()

    init {
        configureWebView()
    }

    /**
     * Configure WebView for JavaScript execution and result extraction.
     */
    private fun configureWebView() {
        webView.apply {
            // Enable JavaScript
            settings.apply {
                javaScriptEnabled = true
                domStorageEnabled = true
                databaseEnabled = true
                cacheMode = android.webkit.WebSettings.LOAD_DEFAULT
                mixedContentMode = android.webkit.WebSettings.MIXED_CONTENT_ALWAYS_ALLOW
                // Allow JS to store data
                setAppCacheEnabled(true)
                javaScriptCanOpenWindowsAutomatically = true
            }

            // Custom WebViewClient to detect page loads and content changes
            webViewClient = object : WebViewClient() {
                override fun shouldInterceptRequest(
                    view: WebView,
                    request: WebResourceRequest
                ): WebResourceResponse? {
                    // Log resource loads for debugging
                    return super.shouldInterceptRequest(view, request)
                }

                override fun onPageFinished(view: WebView, url: String) {
                    super.onPageFinished(view, url)
                    isPageReady = true
                    // Wait a bit for dynamic content to load
                    view.postDelayed({
                        extractPageContent(view)
                    }, 2000)
                }
            }

            // Inject JS interface for communication
            addJavascriptInterface(
                JSInterface(this::onJSContentReady),
                "ContentExtractor"
            )
        }
    }

    /**
     * Extract rendered page content via JavaScript.
     */
    private fun extractPageContent(webView: WebView) {
        webView.evaluateJavascript(
            """
            (function() {
                const html = document.documentElement.outerHTML;
                window.ContentExtractor.onContentReady(html);
                return true;
            })();
            """.trimIndent()
        ) { _ ->
            // Result handled in JS interface
        }
    }

    /**
     * Load a URL and wait for JavaScript to render content.
     * Timeout after 10 seconds if page doesn't load.
     */
    suspend fun loadUrlWithJavaScript(
        url: String,
        query: String? = null,
        timeoutMs: Long = 10000
    ): String = withTimeout(timeoutMs) {
        suspendCancellableCoroutine { continuation ->
            readySignal.thenAccept { content ->
                continuation.resume(content)
            }.exceptionally { e ->
                continuation.resumeWithException(e)
                null
            }

            // Load the URL
            webView.loadUrl(url)

            // If no content after timeout, return current HTML
            webView.postDelayed({
                if (!readySignal.isDone) {
                    extractPageContent(webView)
                }
            }, timeoutMs - 1000)
        }
    }

    /**
     * Inject search query via JavaScript and wait for results.
     */
    suspend fun injectSearchAndWait(
        searchSelector: String,
        submitSelector: String,
        query: String,
        resultSelector: String,
        timeoutMs: Long = 15000
    ): String = withTimeout(timeoutMs) {
        suspendCancellableCoroutine { continuation ->
            val js = """
            (function() {
                // Find and fill search input
                const searchInput = document.querySelector('$searchSelector');
                if (searchInput) {
                    searchInput.value = '$query';
                    searchInput.dispatchEvent(new Event('input', { bubbles: true }));
                    searchInput.dispatchEvent(new Event('change', { bubbles: true }));
                }

                // Click submit button
                const submitBtn = document.querySelector('$submitSelector');
                if (submitBtn) {
                    submitBtn.click();
                }

                // Wait for results to load
                const waitForResults = setInterval(() => {
                    const results = document.querySelectorAll('$resultSelector');
                    if (results.length > 0) {
                        clearInterval(waitForResults);
                        const html = document.documentElement.outerHTML;
                        window.ContentExtractor.onContentReady(html);
                    }
                }, 500);

                // Timeout after 14 seconds
                setTimeout(() => {
                    clearInterval(waitForResults);
                    const html = document.documentElement.outerHTML;
                    window.ContentExtractor.onContentReady(html);
                }, 14000);
            })();
            """.trimIndent()

            readySignal.thenAccept { content ->
                continuation.resume(content)
            }.exceptionally { e ->
                continuation.resumeWithException(e)
                null
            }

            webView.evaluateJavascript(js) { _ ->
                // Result handled in JS interface
            }
        }
    }

    /**
     * Extract all visible URLs from current page (for result links).
     */
    suspend fun extractAllLinks(selector: String = "a[href]"): List<String> {
        return suspendCancellableCoroutine { continuation ->
            webView.evaluateJavascript(
                """
                (function() {
                    const links = Array.from(document.querySelectorAll('$selector'))
                        .map(a => a.href)
                        .filter(href => href && href.startsWith('http'));
                    return JSON.stringify(links);
                })();
                """.trimIndent()
            ) { result ->
                try {
                    val json = result?.trim('"')?.replace("\\\"", "\"") ?: "[]"
                    val links = kotlinx.serialization.json.Json.decodeFromString<List<String>>(json)
                    continuation.resume(links)
                } catch (e: Exception) {
                    continuation.resume(emptyList())
                }
            }
        }
    }

    /**
     * Handle scrolling for infinite-scroll providers.
     */
    suspend fun scrollToBottom(scrollCount: Int = 3) {
        repeat(scrollCount) {
            suspendCancellableCoroutine<Unit> { continuation ->
                webView.evaluateJavascript(
                    "window.scrollBy(0, window.innerHeight); true;"
                ) { _ ->
                    continuation.resume(Unit)
                }
                webView.postDelayed({ continuation.resume(Unit) }, 1500)
            }
        }
    }

    /**
     * JS Interface callback for content extraction.
     */
    private fun onJSContentReady(html: String) {
        pageContent = html
        if (!readySignal.isDone) {
            readySignal.complete(html)
        }
    }

    /**
     * Clear and reset for next operation.
     */
    fun reset() {
        isPageReady = false
        pageContent = ""
        readySignal.complete("")
    }

    /**
     * Cleanup WebView.
     */
    fun destroy() {
        webView.stopLoading()
        webView.destroy()
    }

    /**
     * JavaScript Interface for content communication.
     */
    inner class JSInterface(
        private val onContentReady: (String) -> Unit
    ) {
        @android.webkit.JavascriptInterface
        fun onContentReady(html: String) {
            onContentReady.invoke(html)
        }
    }
}
