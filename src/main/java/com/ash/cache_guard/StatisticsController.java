package com.ash.cache_guard;

import java.util.List;
import java.util.Map;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/statistics")
public class StatisticsController {

    private final ItemService itemService;

    public StatisticsController(ItemService itemService) {
        this.itemService = itemService;
    }

    @GetMapping("/cache")
    public Map<String, Object> getCacheStatistics() {
        return itemService.getCacheStats();
    }

    @GetMapping("/database")
    public Map<String, Object> getDatabaseStatistics() {
        return itemService.getDatabaseStats();
    }

    @PostMapping("/reset-metrics")
    public Map<String, String> resetMetrics() {
        itemService.resetAllStats();
        return Map.of(
                "status", "success",
                "message", "Database metrics and cache statistics reset"
        );
    }

    @GetMapping("/controller")
    public List<AdaptiveConcurrencyController.Decision> getControllerHistory() {
        return itemService.getControllerHistory();
    }
}
