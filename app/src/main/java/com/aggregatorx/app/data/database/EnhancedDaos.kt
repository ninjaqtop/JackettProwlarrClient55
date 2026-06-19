package com.aggregatorx.app.data.database

import androidx.room.*
import com.aggregatorx.app.data.model.*
import kotlinx.coroutines.flow.Flow
import kotlinx.serialization.Serializable // <-- Added this to fix the @Serializable errors

/**
 * ENHANCED ProviderDao Extensions
 * Adds metrics tracking, pattern persistence, and WebView performance analytics
 */
@Dao
interface ProviderDao {
    @Query("SELECT * FROM providers ORDER BY name ASC")
    fun getAllProviders(): Flow<List<Provider>>
    
    @Query("SELECT * FROM providers WHERE isEnabled = 1 ORDER BY name ASC")
    fun getEnabledProviders(): Flow<List<Provider>>
    
    @Query("SELECT * FROM providers WHERE isEnabled = 1")
    suspend fun getEnabledProvidersSync(): List<Provider>
    
    @Query("SELECT * FROM providers WHERE id = :id")
    suspend fun getProviderById(id: String): Provider?
    
    @Query("SELECT * FROM providers WHERE url = :url")
    suspend fun getProviderByUrl(url: String): Provider?
    
    @Insert(onConflict = OnConflictStrategy.REPLACE)
    suspend fun insertProvider(provider: Provider)
    
    @Insert(onConflict = OnConflictStrategy.REPLACE)
    suspend fun insertProviders(providers: List<Provider>)
    
    @Update
    suspend fun updateProvider(provider: Provider)
    
    @Delete
    suspend fun deleteProvider(provider: Provider)
    
    @Query("DELETE FROM providers WHERE id = :id")
    suspend fun deleteProviderById(id: String)
    
    @Query("UPDATE providers SET isEnabled = :enabled WHERE id = :id")
    suspend fun setProviderEnabled(id: String, enabled: Boolean)
    
    @Query("UPDATE providers SET healthScore = :score, avgResponseTime = :responseTime WHERE id = :id")
    suspend fun updateProviderStats(id: String, score: Float, responseTime: Long)
    
    @Query("UPDATE providers SET totalSearches = totalSearches + 1 WHERE id = :id")
    suspend fun incrementSearchCount(id: String)
    
    @Query("UPDATE providers SET failedSearches = failedSearches + 1 WHERE id = :id")
    suspend fun incrementFailedCount(id: String)
    
    @Query("UPDATE providers SET lastAnalyzed = :timestamp WHERE id = :id")
    suspend fun updateLastAnalyzed(id: String, timestamp: Long)

    // ── ENHANCED: Metrics Tracking ──────────────────────────────────────────
    
    /**
     * Update comprehensive provider metrics after each search
     */
    @Query("""
        UPDATE providers SET
            totalSearches = totalSearches + 1,
            avgResponseTime = CASE 
                WHEN totalSearches > 0 
                THEN (avgResponseTime * totalSearches + :responseTimeMs) / (totalSearches + 1)
                ELSE :responseTimeMs
            END,
            failedSearches = CASE WHEN :success = 0 THEN failedSearches + 1 ELSE failedSearches END,
            successRate = CASE 
                WHEN totalSearches > 0 
                THEN ((totalSearches - failedSearches + CASE WHEN :success = 1 THEN 1 ELSE 0 END) * 100.0) / (totalSearches + 1)
                ELSE CASE WHEN :success = 1 THEN 100.0 ELSE 0.0 END
            END,
            healthScore = (successRate * 0.6) + ((CASE WHEN avgResponseTime < 5000 THEN 100 WHEN avgResponseTime < 10000 THEN 75 ELSE 50 END) * 0.4)
        WHERE id = :providerId
    """)
    suspend fun updateProviderMetrics(
        providerId: String,
        success: Boolean,
        responseTimeMs: Long
    )

    /**
     * Update WebView success metrics
     */
    @Query("""
        UPDATE providers SET
            webViewAttempts = webViewAttempts + 1,
            lastWebViewSearch = :timestamp,
            webViewSuccessRate = CASE
                WHEN webViewAttempts > 0
                THEN CASE WHEN :success = 1 
                    THEN (webViewSuccessRate * webViewAttempts + 100) / (webViewAttempts + 1)
                    ELSE (webViewSuccessRate * webViewAttempts) / (webViewAttempts + 1)
                END
                ELSE CASE WHEN :success = 1 THEN 100.0 ELSE 0.0 END
            END
        WHERE id = :providerId
    """)
    suspend fun updateWebViewMetrics(
        providerId: String,
        success: Boolean,
        timestamp: Long = System.currentTimeMillis()
    )

    /**
     * Get providers ranked by search success rate
     */
    @Query("""
        SELECT * FROM providers 
        WHERE isEnabled = 1 
        ORDER BY successRate DESC, healthScore DESC
        LIMIT :limit
    """)
    suspend fun getTopProvidersSync(limit: Int = 10): List<Provider>

    /**
     * Get providers that need re-analysis
     * Prioritizes providers without analysis, then by oldest lastAnalyzed timestamp
     */
    @Query("""
        SELECT * FROM providers 
        WHERE isEnabled = 1 AND (lastAnalyzed IS NULL OR lastAnalyzed < :thresholdMs)
        ORDER BY CASE WHEN lastAnalyzed IS NULL THEN 0 ELSE 1 END, lastAnalyzed ASC
    """)
    suspend fun getProvidersNeedingAnalysis(thresholdMs: Long): List<Provider>
}

@Dao
interface SiteAnalysisDao {
    @Query("SELECT * FROM site_analysis ORDER BY analyzedAt DESC")
    fun getAllAnalyses(): Flow<List<SiteAnalysis>>
    
    @Query("SELECT * FROM site_analysis WHERE providerId = :providerId ORDER BY analyzedAt DESC LIMIT 1")
    suspend fun getLatestAnalysis(providerId: String): SiteAnalysis?
    
    @Query("SELECT * FROM site_analysis WHERE id = :id")
    suspend fun getAnalysisById(id: String): SiteAnalysis?
    
    @Insert(onConflict = OnConflictStrategy.REPLACE)
    suspend fun insertAnalysis(analysis: SiteAnalysis)
    
    @Update
    suspend fun updateAnalysis(analysis: SiteAnalysis)
    
    @Delete
    suspend fun deleteAnalysis(analysis: SiteAnalysis)
    
    @Query("DELETE FROM site_analysis WHERE providerId = :providerId")
    suspend fun deleteAnalysesForProvider(providerId: String)
}

@Dao
interface ScrapingConfigDao {
    @Query("SELECT * FROM scraping_configs")
    fun getAllConfigs(): Flow<List<ScrapingConfig>>
    
    @Query("SELECT * FROM scraping_configs WHERE providerId = :providerId")
    suspend fun getConfigForProvider(providerId: String): ScrapingConfig?
    
    @Insert(onConflict = OnConflictStrategy.REPLACE)
    suspend fun insertConfig(config: ScrapingConfig)
    
    @Update
    suspend fun updateConfig(config: ScrapingConfig)
    
    @Delete
    suspend fun deleteConfig(config: ScrapingConfig)
    
    @Query("DELETE FROM scraping_configs WHERE providerId = :providerId")
    suspend fun deleteConfigForProvider(providerId: String)
}

/**
 * ENHANCED: Pattern Learning & Persistence DAO
 * Stores discovered site patterns for intelligent crawling
 */
@Entity(tableName = "site_patterns")
@Serializable
data class SitePattern(
    @PrimaryKey
    val id: String = java.util.UUID.randomUUID().toString(),
    val providerId: String,
    val patternType: String, // "pagination", "infinite_scroll", "load_more", etc.
    val selector: String,
    val successRate: Float = 1.0f,
    val usageCount: Int = 1,
    val lastUsed: Long = System.currentTimeMillis(),
    val discovered: Long = System.currentTimeMillis(),
    val metadata: String = "{}" // JSON for additional properties
)

@Dao
interface SitePatternDao {
    @Query("SELECT * FROM site_patterns WHERE providerId = :providerId ORDER BY successRate DESC, usageCount DESC")
    suspend fun getPatternsForProvider(providerId: String): List<SitePattern>

    @Query("SELECT * FROM site_patterns WHERE providerId = :providerId AND patternType = :type LIMIT 1")
    suspend fun getPatternByType(providerId: String, type: String): SitePattern?

    @Insert(onConflict = OnConflictStrategy.REPLACE)
    suspend fun insertPattern(pattern: SitePattern)

    @Update
    suspend fun updatePattern(pattern: SitePattern)

    @Query("""
        UPDATE site_patterns SET 
            successRate = (successRate * usageCount + :success) / (usageCount + 1),
            usageCount = usageCount + 1,
            lastUsed = :timestamp
        WHERE id = :patternId
    """)
    suspend fun recordPatternUsage(patternId: String, success: Boolean, timestamp: Long = System.currentTimeMillis())

    @Query("DELETE FROM site_patterns WHERE providerId = :providerId")
    suspend fun deletePattersForProvider(providerId: String)

    @Query("DELETE FROM site_patterns WHERE lastUsed < :olderThan")
    suspend fun deleteStalePatterns(olderThan: Long)
}

/**
 * ENHANCED: Search Performance Analytics DAO
 * Tracks search performance metrics for optimization
 */
@Entity(tableName = "search_analytics")
@Serializable
data class SearchAnalytics(
    @PrimaryKey
    val id: String = java.util.UUID.randomUUID().toString(),
    val query: String,
    val providerId: String,
    val resultCount: Int,
    val responseTimeMs: Long,
    val success: Boolean,
    val strategy: String, // "html_parsing", "webview", "js_injection", etc.
    val usedFallback: Boolean = false,
    val timestamp: Long = System.currentTimeMillis()
)

@Dao
interface SearchAnalyticsDao {
    @Insert(onConflict = OnConflictStrategy.REPLACE)
    suspend fun insertAnalytic(analytic: SearchAnalytics)

    @Query("""
        SELECT * FROM search_analytics 
        WHERE providerId = :providerId 
        ORDER BY timestamp DESC 
        LIMIT :limit
    """)
    suspend fun getProviderSearchHistory(providerId: String, limit: Int = 100): List<SearchAnalytics>

    @Query("""
        SELECT AVG(responseTimeMs) as avgTime, 
               COUNT(*) as total,
               SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) as successful
        FROM search_analytics 
        WHERE providerId = :providerId
    """)
    suspend fun getProviderAnalytics(providerId: String): ProviderSearchStats?

    @Query("DELETE FROM search_analytics WHERE timestamp < :olderThan")
    suspend fun deleteOldAnalytics(olderThan: Long)
}

data class ProviderSearchStats(
    val avgTime: Long,
    val total: Int,
    val successful: Int
)
