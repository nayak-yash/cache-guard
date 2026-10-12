package com.ash.cache_guard;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/items")
public class ItemController {

    private final ItemService itemService;

    public ItemController(ItemService dataService) {
        this.itemService = dataService;
    }

    @GetMapping("/{key}")
    public ResponseEntity<Item> getData(@PathVariable String key) {
        return itemService.getByKey(key)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }
}