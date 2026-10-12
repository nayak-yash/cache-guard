package com.ash.cache_guard;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import org.springframework.stereotype.Service;

@Service
public class ItemService {

    private final ItemRepository itemRepository;

    private volatile LoadingCache<String, Optional<Item>> cache;

    private final AtomicLong databaseLoadCount = new AtomicLong();

    public ItemService(ItemRepository itemRepository) {
        this.itemRepository = itemRepository;
        this.cache = createCache();
    }

    private LoadingCache<String, Optional<Item>> createCache() {
        return Caffeine.newBuilder()
                .maximumSize(10_000)
                .recordStats()
                .build(key -> {
                    databaseLoadCount.incrementAndGet();
                    return itemRepository.findByKey(key);
                });
    }

    public Optional<Item> getByKey(String key) {
        return cache.get(key);
    }

    public Map<String, Object> getCacheStats() {
        CacheStats stats = cache.stats();
        return Map.of(
                "cacheSize", cache.estimatedSize(),
                "hitCount", stats.hitCount(),
                "missCount", stats.missCount(),
                "hitRate", stats.hitRate(),
                "loadSuccessCount", stats.loadSuccessCount(),
                "loadFailureCount", stats.loadFailureCount(),
                "evictionCount", stats.evictionCount(),
                "databaseLoadCount", databaseLoadCount.get()
        );
    }

    public Map<String, Object> getDatabaseStats() {
        return itemRepository.getQueryStats();
    }

    public void resetAllStats() {
        itemRepository.resetQueryStats();
        databaseLoadCount.set(0);
        cache = createCache();
    }

    public List<AdaptiveConcurrencyController.Decision> getControllerHistory() {
        return itemRepository.getControllerHistory();
    }
}