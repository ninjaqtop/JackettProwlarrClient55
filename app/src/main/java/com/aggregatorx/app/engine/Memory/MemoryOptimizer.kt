package com.aggregatorx.app.engine.memory

import android.app.ActivityManager
import android.content.ComponentCallbacks2
import android.content.Context
import android.content.res.Configuration
import androidx.lifecycle.DefaultLifecycleObserver
import java.util.concurrent.ConcurrentHashMap
import javax.inject.Inject
import javax.inject.Singleton

@Singleton
class MemoryOptimizer @Inject constructor(
    private val context: Context
) : DefaultLifecycleObserver, ComponentCallbacks2 {

    private val activityManager = context.getSystemService(Context.ACTIVITY_SERVICE) as ActivityManager
    private val cacheManagers = ConcurrentHashMap<String, () -> Unit>()
    
    fun registerCacheManager(id: String, cleanup: () -> Unit) {
        cacheManagers[id] = cleanup
    }
    
    fun unregisterCacheManager(id: String) {
        cacheManagers.remove(id)
    }

    fun isMemoryPressured(): Boolean {
        val runtime = Runtime.getRuntime()
        val usedMemory = runtime.totalMemory() - runtime.freeMemory()
        val maxMemory = runtime.maxMemory()
        val percentUsed = (usedMemory.toFloat() / maxMemory) * 100f
        
        return percentUsed > 70f
    }

    fun getNativeMemoryInfo(): MemoryInfo {
        val memInfo = ActivityManager.MemoryInfo()
        activityManager.getMemoryInfo(memInfo)
        
        val totalMemory = memInfo.totalMem
        return MemoryInfo(
            totalMemory = totalMemory,
            availableMemory = memInfo.availMem,
            isLowMemory = memInfo.lowMemory,
            percentUsed = if (totalMemory > 0) 
                ((totalMemory - memInfo.availMem).toFloat() / totalMemory * 100f) 
            else 0f
        )
    }

    fun emergencyCleanup() {
        cacheManagers.forEach { (_, cleanup) ->
            try { cleanup() } catch (_: Exception) {}
        }
        System.gc()
    }

    override fun onTrimMemory(level: Int) {
        when (level) {
            ComponentCallbacks2.TRIM_MEMORY_RUNNING_CRITICAL -> emergencyCleanup()
            ComponentCallbacks2.TRIM_MEMORY_RUNNING_MODERATE -> {
                cacheManagers.forEach { (_, cleanup) ->
                    try { cleanup() } catch (_: Exception) {}
                }
            }
        }
    }

    override fun onConfigurationChanged(newConfig: Configuration) {
        // Handle configuration changes if needed
    }

    override fun onLowMemory() {
        emergencyCleanup()
    }

    data class MemoryInfo(
        val totalMemory: Long,
        val availableMemory: Long,
        val isLowMemory: Boolean,
        val percentUsed: Float
    )
}
