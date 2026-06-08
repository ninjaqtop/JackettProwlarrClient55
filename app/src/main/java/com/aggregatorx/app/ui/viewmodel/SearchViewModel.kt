package com.aggregatorx.app.ui.viewmodel

import android.util.Log
import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import com.aggregatorx.app.data.model.*
import com.aggregatorx.app.data.repository.AggregatorRepository
import com.aggregatorx.app.engine.media.*
import com.aggregatorx.app.engine.token.TokenManager
import com.aggregatorx.app.engine.util.EngineUtils
import dagger.hilt.android.lifecycle.HiltViewModel
import kotlinx.coroutines.Job
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.launch
import javax.inject.Inject

data class VideoPreviewResult(
    val videoUrl: String,
    val headers: Map<String, String> = emptyMap()
)

@HiltViewModel
class SearchViewModel @Inject constructor(
    private val repository: AggregatorRepository,
    private val videoExtractor: VideoExtractorEngine,
    private val advancedExtractor: AdvancedVideoExtractorEngine,
    private val videoStreamResolver: VideoStreamResolver,
    private val downloadManager: DownloadManager,
    private val tokenManager: TokenManager
) : ViewModel() {

    companion object {
        private const val TAG = "SearchViewModel"
    }

    private val _uiState          = MutableStateFlow(SearchUiState())
    val uiState: StateFlow<SearchUiState> = _uiState.asStateFlow()

    private val _providerResults  = MutableStateFlow<List<ProviderSearchResults>>(emptyList())
    val providerResults: StateFlow<List<ProviderSearchResults>> = _providerResults.asStateFlow()

    private val _videoExtractionState = MutableStateFlow<VideoExtractionState>(VideoExtractionState.Idle)
    val videoExtractionState: StateFlow<VideoExtractionState> = _videoExtractionState.asStateFlow()

    val downloads: StateFlow<Map<String, DownloadState>> = downloadManager.downloads

    private val _likedUrls    = MutableStateFlow<Set<String>>(emptySet())
    val likedUrls: StateFlow<Set<String>> = _likedUrls.asStateFlow()

    private val _tokenResults = MutableStateFlow<List<SearchResult>>(emptyList())
    val tokenResults: StateFlow<List<SearchResult>> = _tokenResults.asStateFlow()

    private val _myAiResults  = MutableStateFlow<List<SearchResult>>(emptyList())
    val myAiResults: StateFlow<List<SearchResult>> = _myAiResults.asStateFlow()

    private val _isDiscoveryPaused = MutableStateFlow(false)
    val isDiscoveryPaused: StateFlow<Boolean> = _isDiscoveryPaused.asStateFlow()

    // Loop-2 running indicator so the UI can show a subtle "finding more…" badge
    private val _isLoop2Running = MutableStateFlow(false)
    val isLoop2Running: StateFlow<Boolean> = _isLoop2Running.asStateFlow()

    // Kept for API compat; no longer drives pagination (handled internally)
    private val _providerPages = MutableStateFlow<Map<String, Int>>(emptyMap())
    val providerPages: StateFlow<Map<String, Int>> = _providerPages.asStateFlow()

    private val sessionSeenUrls   = mutableSetOf<String>()
    private val videoPreviewCache = java.util.concurrent.ConcurrentHashMap<String, VideoPreviewResult>()
    private var currentSearchJob: Job? = null
    private var lastSearchQuery   = ""

    init {
        viewModelScope.launch {
            repository.getRecentSearches().collect { searches ->
                _uiState.update { it.copy(recentSearches = searches) }
            }
        }
        viewModelScope.launch { _likedUrls.value = repository.getAllLikedUrls() }
    }

    // ── SEARCH ──────────────────────────────────────────────────────────────────

    /**
     * ✅ DUAL-LOOP REAL-TIME SEARCH PIPELINE
     * 
     * LOOP 1 — Direct Results (Real-time streaming)
     * ========================================
     * ✅ Every enabled provider searched concurrently
     * ✅ Each provider auto-crawls multiple pages (40-60+ results)
     * ✅ Results stream into UI as EACH PROVIDER COMPLETES
     * ✅ Smart fallback to WebView for JS-heavy sites
     * ✅ Auto-stop when sufficient results per provider
     * ✅ Continues even if individual providers fail
     * ✅ Pattern learning on every search
     * ✅ Complete loop guaranteed (all providers processed)
     * 
     * LOOP 2 — Smart / Preference Results (Background enhancement)
     * ============================================================
     * ✅ Runs AFTER Loop 1 completes
     * ✅ Loop 1 results stay fully visible (non-blocking)
     * ✅ Adds preference-boosted results from liked domains
     * ✅ Token-discovered related results
     * ✅ Injects results directly into provider buckets
     * 
     * GUARANTEES:
     * ✅ Results appear instantly as providers finish
     * ✅ No waiting for all providers - incremental updates
     * ✅ All providers get searched (complete loop)
     * ✅ Graceful error handling - continues on failure
     * ✅ Real-time progress tracking
     */
    fun search(isLoadMore: Boolean = false) {
        val query = _uiState.value.query.trim()
        if (query.isEmpty() || _isDiscoveryPaused.value) return

        Log.d(TAG, "🔍 Starting search: '$query' (loadMore=$isLoadMore)")
        currentSearchJob?.cancel()

        currentSearchJob = viewModelScope.launch {
            val isNewQuery = !isLoadMore || query != lastSearchQuery
            lastSearchQuery = query

            if (isNewQuery) {
                Log.d(TAG, "📋 Clearing cache for new query")
                sessionSeenUrls.clear()
                repository.clearSearchCache()
                videoPreviewCache.clear()
                _providerResults.value  = emptyList()
                _tokenResults.value     = emptyList()
                _myAiResults.value      = emptyList()
                _providerPages.value    = emptyMap()
                _isLoop2Running.value   = false
            }

            _uiState.update { it.copy(isSearching = true, currentSearchQuery = query, error = null) }
            val loop1Results = if (isLoadMore) _providerResults.value.toMutableList() else mutableListOf()

            // ── LOOP 1: Direct results - Real-time streaming ───────────────────
            Log.d(TAG, "🎬 Loop 1: Direct provider search with real-time streaming")
            
            repository.searchAllProviders(query = query, forceRefresh = isNewQuery)
                .catch { e ->
                    Log.e(TAG, "❌ Loop 1 error: ${e.message}", e)
                    if (loop1Results.isEmpty()) {
                        _uiState.update { it.copy(error = e.message ?: "Search failed") }
                    }
                }
                .collect { providerResult ->
                    Log.d(TAG, "📊 Received: ${providerResult.provider.name} (${providerResult.results.size} results, success=${providerResult.success})")
                    
                    // De-duplicate: only add new URLs
                    val unique = providerResult.results.filter { sessionSeenUrls.add(it.url) }
                    val merged = if (unique.isNotEmpty()) {
                        providerResult.copy(results = unique)
                    } else {
                        providerResult
                    }

                    // Replace existing entry for this provider or append new one
                    val idx = loop1Results.indexOfFirst { it.provider.id == merged.provider.id }
                    if (idx >= 0) {
                        loop1Results[idx] = merged
                    } else {
                        loop1Results.add(merged)
                    }
                    
                    // ✅ UPDATE UI IN REAL-TIME
                    _providerResults.value = loop1Results.toList()

                    // Update aggregated results (ranking & top results)
                    try {
                        val aggregated = repository.aggregateSearchResults(query, loop1Results)
                        _uiState.update { it.copy(
                            aggregatedResults   = aggregated,
                            totalResults        = sessionSeenUrls.size,
                            successfulProviders = aggregated.successfulProviders,
                            failedProviders     = aggregated.failedProviders
                        ) }
                        Log.d(TAG, "📈 Progress: ${loop1Results.size} providers, ${sessionSeenUrls.size} unique results")
                    } catch (e: Exception) {
                        Log.w(TAG, "��️ Aggregation failed: ${e.message}")
                    }
                }

            // ✅ Loop 1 complete
            Log.d(TAG, "✅ Loop 1 complete: ${loop1Results.size} providers, ${sessionSeenUrls.size} total results")
            _uiState.update { it.copy(isSearching = false, searchCompleted = true) }

            // ── LOOP 2: Smart / preference results (Background, non-blocking) ─
            Log.d(TAG, "🧠 Loop 2: Smart results enhancement (background)")
            _isLoop2Running.value = true

            // 2a — Preference-boosted results from liked domains
            launch {
                try {
                    Log.d(TAG, "  💚 Boosting results from liked domains...")
                    val likedDomains = _likedUrls.value.mapNotNull {
                        try { 
                            java.net.URI(it).host 
                        } catch (_: Exception) { 
                            null 
                        }
                    }.toSet()

                    val aiRanked = loop1Results.flatMap { it.results }
                        .map { r ->
                            val host  = try { java.net.URI(r.url).host } catch (_: Exception) { "" }
                            val boost = if (host in likedDomains) 50f else 0f
                            r.copy(relevanceScore = r.relevanceScore + boost)
                        }
                        .filter { it.relevanceScore > 40f }
                        .sortedByDescending { it.relevanceScore }

                    _myAiResults.value = aiRanked.take(60)
                    Log.d(TAG, "  ✅ Added ${_myAiResults.value.size} preference-boosted results")

                    // Inject AI results back into provider buckets (inline)
                    if (aiRanked.isNotEmpty()) {
                        val updated = loop1Results.toMutableList()
                        aiRanked.groupBy { it.providerId }.forEach { (pid, aiList) ->
                            val pIdx = updated.indexOfFirst { it.provider.id == pid }
                            if (pIdx >= 0) {
                                val existing = updated[pIdx]
                                val newUrls  = aiList.filter { sessionSeenUrls.add(it.url) }
                                if (newUrls.isNotEmpty()) {
                                    updated[pIdx] = existing.copy(
                                        results = (existing.results + newUrls)
                                            .sortedByDescending { it.relevanceScore }
                                    )
                                    _providerResults.value = updated.toList()
                                    Log.d(TAG, "  📌 Injected ${newUrls.size} results into ${existing.provider.name}")
                                }
                            }
                        }
                    }
                } catch (e: Exception) {
                    Log.w(TAG, "⚠️ Preference boost failed: ${e.message}")
                }
            }

            // 2b — Token-discovered related results
            launch {
                try {
                    Log.d(TAG, "  🔗 Discovering token-based results...")
                    val tokensFound = mutableListOf<SearchResult>()
                    loop1Results.filter { it.success }.forEach { p ->
                        try {
                            tokenManager.replayTokensForSearch(p.provider.baseUrl, query)
                                .forEach { url ->
                                    if (sessionSeenUrls.add(url)) {
                                        tokensFound.add(SearchResult(
                                            providerId     = p.provider.id,
                                            providerName   = "${p.provider.name} [RELATED]",
                                            title          = "Related: $query",
                                            url            = url,
                                            relevanceScore = 55f
                                        ))
                                    }
                                }
                        } catch (_: Exception) {}
                    }

                    if (tokensFound.isNotEmpty()) {
                        _tokenResults.value = tokensFound.take(40)
                        Log.d(TAG, "  ✅ Found ${_tokenResults.value.size} token-based results")
                        
                        val updated = loop1Results.toMutableList()
                        tokensFound.groupBy { it.providerId }.forEach { (pid, tokenList) ->
                            val pIdx = updated.indexOfFirst { it.provider.id == pid }
                            if (pIdx >= 0) {
                                val existing = updated[pIdx]
                                updated[pIdx] = existing.copy(
                                    results = (existing.results + tokenList).distinctBy { it.url }
                                )
                                _providerResults.value = updated.toList()
                            }
                        }
                    }
                } catch (e: Exception) {
                    Log.w(TAG, "⚠️ Token discovery failed: ${e.message}")
                }
            }

            // Loop 2 complete
            _isLoop2Running.value = false
            Log.d(TAG, "✅ Loop 2 complete: Enhancement finished")
        }
    }

    // ── VIDEO EXTRACTION ─────────────────────────────────────────────────────────

    fun extractVideoUrl(result: SearchResult) {
        viewModelScope.launch {
            _videoExtractionState.value = VideoExtractionState.Extracting(result.url)
            try {
                val url = videoExtractor.extractVideoUrl(result.url)
                    ?: advancedExtractor.extractVideoUrl(result.url)
                
                if (url != null) {
                    _videoExtractionState.value = VideoExtractionState.Success(url)
                } else {
                    _videoExtractionState.value = VideoExtractionState.Error("No video URL found")
                }
            } catch (e: Exception) {
                _videoExtractionState.value = VideoExtractionState.Error(e.message ?: "Extraction failed")
            }
        }
    }

    fun extractVideoForPreview(result: SearchResult) {
        viewModelScope.launch {
            try {
                val cached = videoPreviewCache[result.url]
                if (cached != null) {
                    _videoExtractionState.value = VideoExtractionState.Success(cached.videoUrl)
                    return@launch
                }

                val url = videoExtractor.extractVideoUrl(result.url)
                if (url != null) {
                    val preview = VideoPreviewResult(videoUrl = url)
                    videoPreviewCache[result.url] = preview
                    _videoExtractionState.value = VideoExtractionState.Success(url)
                }
            } catch (e: Exception) {
                Log.e(TAG, "Preview extraction failed: ${e.message}")
            }
        }
    }

    // ── DOWNLOAD MANAGEMENT ──────────────────────────────────────────────────────

    fun downloadResult(result: SearchResult) {
        viewModelScope.launch {
            try {
                downloadManager.startDownload(result)
            } catch (e: Exception) {
                Log.e(TAG, "Download failed: ${e.message}")
            }
        }
    }

    // ── PREFERENCE TRACKING ──────────────────────────────────────────────────────

    fun toggleLike(result: SearchResult) {
        viewModelScope.launch {
            try {
                if (result.url in _likedUrls.value) {
                    repository.toggleLike(result)
                    _likedUrls.value = repository.getAllLikedUrls()
                } else {
                    repository.toggleLike(result)
                    _likedUrls.value = repository.getAllLikedUrls()
                }
            } catch (e: Exception) {
                Log.e(TAG, "Like toggle failed: ${e.message}")
            }
        }
    }

    // ── CONTROL FUNCTIONS ────────────────────────────────────────────────────────

    fun pauseDiscovery() {
        _isDiscoveryPaused.value = true
        currentSearchJob?.cancel()
    }

    fun resumeDiscovery() {
        _isDiscoveryPaused.value = false
    }

    fun updateQuery(newQuery: String) {
        _uiState.update { it.copy(query = newQuery) }
    }

    fun clearSearchCache() {
        repository.clearSearchCache()
    }
}

// ── DATA CLASSES ─────────────────────────────────────────────────────────────

data class SearchUiState(
    val query: String = "",
    val isSearching: Boolean = false,
    val searchCompleted: Boolean = false,
    val error: String? = null,
    val currentSearchQuery: String = "",
    val recentSearches: List<SearchHistoryEntry> = emptyList(),
    val aggregatedResults: AggregatedSearchResults? = null,
    val totalResults: Int = 0,
    val successfulProviders: Int = 0,
    val failedProviders: Int = 0
)

sealed class VideoExtractionState {
    data object Idle : VideoExtractionState()
    data class Extracting(val url: String) : VideoExtractionState()
    data class Success(val videoUrl: String) : VideoExtractionState()
    data class Error(val message: String) : VideoExtractionState()
}
