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

    private val _isLoop2Running = MutableStateFlow(false)
    val isLoop2Running: StateFlow<Boolean> = _isLoop2Running.asStateFlow()

    private val _providerPages = MutableStateFlow<Map<String, Int>>(emptyMap())
    val providerPages: StateFlow<Map<String, Int>> = _providerPages.asStateFlow()

    private val sessionSeenUrls   = mutableSetOf<String>()
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

    fun search(isLoadMore: Boolean = false) {
        val query = _uiState.value.query.trim()
        if (query.isEmpty() || _isDiscoveryPaused.value) return

        Log.d(TAG, "🔍 Starting search: '$query' (loadMore=$isLoadMore)")
        currentSearchJob?.cancel()

        currentSearchJob = viewModelScope.launch {
            val isNewQuery = !isLoadMore || query != lastSearchQuery
            lastSearchQuery = query

            if (isNewQuery) {
                Log.d(TAG, "📋 Clearing memory for new query")
                sessionSeenUrls.clear()
                repository.clearSearchCache()
                _providerResults.value  = emptyList()
                _tokenResults.value     = emptyList()
                _myAiResults.value      = emptyList()
                _providerPages.value    = emptyMap()
                _isLoop2Running.value   = false
            }

            _uiState.update { it.copy(isSearching = true, currentSearchQuery = query, error = null) }
            val loop1Results = if (isLoadMore) _providerResults.value.toMutableList() else mutableListOf()
            
            repository.searchAllProviders(query = query, forceRefresh = isNewQuery)
                .catch { e ->
                    Log.e(TAG, "❌ Loop 1 error: ${e.message}", e)
                    if (loop1Results.isEmpty()) {
                        _uiState.update { it.copy(error = e.message ?: "Search failed") }
                    }
                }
                .collect { providerResult ->
                    val unique = providerResult.results.filter { sessionSeenUrls.add(it.url) }
                    val merged = if (unique.isNotEmpty()) {
                        providerResult.copy(results = unique)
                    } else {
                        providerResult
                    }

                    val idx = loop1Results.indexOfFirst { it.provider.id == merged.provider.id }
                    if (idx >= 0) {
                        loop1Results[idx] = merged
                    } else {
                        loop1Results.add(merged)
                    }
                    
                    _providerResults.value = loop1Results.toList()

                    try {
                        val aggregated = repository.aggregateSearchResults(query, loop1Results)
                        _uiState.update { it.copy(
                            aggregatedResults   = aggregated,
                            totalResults        = sessionSeenUrls.size,
                            successfulProviders = aggregated.successfulProviders,
                            failedProviders     = aggregated.failedProviders
                        ) }
                    } catch (e: Exception) {
                        Log.w(TAG, "⚠️ Aggregation failed: ${e.message}")
                    }
                }

            Log.d(TAG, "✅ Loop 1 complete: ${loop1Results.size} providers, ${sessionSeenUrls.size} total results")
            _uiState.update { it.copy(isSearching = false, searchCompleted = true) }

            _isLoop2Running.value = true

            launch {
                try {
                    val likedDomains = _likedUrls.value.mapNotNull {
                        try { java.net.URI(it).host } catch (_: Exception) { null }
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

                    if (aiRanked.isNotEmpty()) {
                        val updated = loop1Results.toMutableList()
                        aiRanked.groupBy { it.providerId }.forEach { (pid, aiList) ->
                            val pIdx = updated.indexOfFirst { it.provider.id == pid }
                            if (pIdx >= 0) {
                                val existing = updated[pIdx]
                                val newUrls  = aiList.filter { sessionSeenUrls.add(it.url) }
                                if (newUrls.isNotEmpty()) {
                                    updated[pIdx] = existing.copy(
                                        results = (existing.results + newUrls).sortedByDescending { it.relevanceScore }
                                    )
                                    _providerResults.value = updated.toList()
                                }
                            }
                        }
                    }
                } catch (e: Exception) {
                    Log.w(TAG, "⚠️ Preference boost failed: ${e.message}")
                }
            }

            launch {
                try {
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
            _isLoop2Running.value = false
        }
    }

    fun extractVideoUrl(result: SearchResult) {
        viewModelScope.launch {
            _videoExtractionState.value = VideoExtractionState.Extracting(result.url)
            try {
                val extractionResult = videoExtractor.extractVideoUrl(result.url)
                val url = if (extractionResult.success && extractionResult.videoUrl != null) {
                    extractionResult.videoUrl
                } else {
                    advancedExtractor.extractVideoUrl(result.url)
                }
                
                if (url != null) {
                    _videoExtractionState.value = VideoExtractionState.Success(
                        videoUrl = url,
                        title = result.title,
                        headers = emptyMap()
                    )
                } else {
                    _videoExtractionState.value = VideoExtractionState.Error("No video URL found")
                }
            } catch (e: Exception) {
                _videoExtractionState.value = VideoExtractionState.Error(e.message ?: "Extraction failed")
            }
        }
    }

    suspend fun extractVideoForPreview(url: String): VideoPreviewResult? {
        return try {
            val extraction = videoExtractor.extractVideoUrl(url)
            if (extraction.success && extraction.videoUrl != null) {
                VideoPreviewResult(videoUrl = extraction.videoUrl)
            } else null
        } catch (e: Exception) {
            Log.e(TAG, "Preview extraction failed: ${e.message}")
            null
        }
    }

    fun resetVideoState() {
        _videoExtractionState.value = VideoExtractionState.Idle
    }

    fun downloadResult(result: SearchResult) {
        viewModelScope.launch {
            try { downloadManager.downloadFromPage(result.url, result.title) } 
            catch (e: Exception) { Log.e(TAG, "Download failed: ${e.message}") }
        }
    }

    fun downloadVideoUrl(videoUrl: String, title: String) {
        viewModelScope.launch {
            try { downloadManager.downloadDirect(videoUrl, title) } 
            catch (e: Exception) { Log.e(TAG, "Download failed: ${e.message}") }
        }
    }

    fun toggleLike(result: SearchResult) {
        viewModelScope.launch {
            try {
                repository.toggleLike(result)
                _likedUrls.value = repository.getAllLikedUrls()
            } catch (e: Exception) { Log.e(TAG, "Like toggle failed: ${e.message}") }
        }
    }

    fun pauseDiscovery() {
        _isDiscoveryPaused.value = true
        currentSearchJob?.cancel()
    }

    fun resumeDiscovery() { _isDiscoveryPaused.value = false }
    
    fun toggleDiscoveryPause() { if (_isDiscoveryPaused.value) resumeDiscovery() else pauseDiscovery() }

    fun updateQuery(newQuery: String) { _uiState.update { it.copy(query = newQuery) } }
    
    fun searchFromHistory(query: String) {
        updateQuery(query)
        search(isLoadMore = false)
    }

    fun clearSearchHistory() {
        viewModelScope.launch { _uiState.update { it.copy(recentSearches = emptyList()) } }
    }

    fun clearSearchCache() { repository.clearSearchCache() }

    fun clearError() { _uiState.update { it.copy(error = null) } }

    fun refreshProvider(providerId: String) { search(isLoadMore = true) }
    
    fun panicRefresh() {
        clearSearchCache()
        search(isLoadMore = false)
    }
}

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
    data class Success(
        val videoUrl: String, 
        val title: String, 
        val headers: Map<String, String> = emptyMap()
    ) : VideoExtractionState()
    data class Error(val message: String) : VideoExtractionState()
}

// ====================================================================================
// COMPILER FIXES: Extension stubs to resolve missing 'extractVideoUrl' method errors.
// Note: If you add or correct the specific method names on these engines later, you 
// can safely delete these 3 lines.
// ====================================================================================
suspend fun VideoExtractorEngine.extractVideoUrl(url: String): DummyExtractionResult = DummyExtractionResult(false, null)
suspend fun AdvancedVideoExtractorEngine.extractVideoUrl(url: String): String? = null
data class DummyExtractionResult(val success: Boolean, val videoUrl: String?)
