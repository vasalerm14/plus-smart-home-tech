package ru.yandex.practicum.inventory.service;

import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.inventory.dto.InventoryDto;
import ru.yandex.practicum.inventory.dto.ReserveRequest;
import ru.yandex.practicum.inventory.dto.ReserveResponse;
import ru.yandex.practicum.inventory.dto.UpdateInventoryRequest;
import ru.yandex.practicum.inventory.entity.Inventory;
import ru.yandex.practicum.inventory.exception.InsufficientStockException;
import ru.yandex.practicum.inventory.exception.NotFoundException;
import ru.yandex.practicum.inventory.mapper.InventoryMapper;
import ru.yandex.practicum.inventory.repository.InventoryRepository;

import java.util.List;

@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class InventoryService {

    private final InventoryRepository inventoryRepository;

    public List<InventoryDto> getAll() {
        return inventoryRepository.findAll(Sort.by("id")).stream()
                .map(InventoryMapper::toDto)
                .toList();
    }

    public InventoryDto getByProductId(Long productId) {
        return InventoryMapper.toDto(findInventory(productId));
    }

    @Transactional
    public InventoryDto create(UpdateInventoryRequest request) {
        if (inventoryRepository.existsByProductId(request.productId())) {
            throw new IllegalArgumentException(
                    "Запись об остатках товара с id=" + request.productId() + " уже существует");
        }
        return InventoryMapper.toDto(inventoryRepository.save(InventoryMapper.toEntity(request)));
    }

    @Transactional
    public InventoryDto update(UpdateInventoryRequest request) {
        Inventory inventory = findInventory(request.productId());
        if (request.quantity() < inventory.getReservedQuantity()) {
            throw new IllegalArgumentException(
                    "Количество не может быть меньше зарезервированного: " + inventory.getReservedQuantity());
        }
        inventory.setQuantity(request.quantity());
        return InventoryMapper.toDto(inventoryRepository.saveAndFlush(inventory));
    }

    @Transactional
    public ReserveResponse reserve(ReserveRequest request) {
        Inventory inventory = findInventory(request.productId());
        if (inventory.getAvailableQuantity() < request.quantity()) {
            throw new InsufficientStockException("Недостаточно товара с id=" + request.productId()
                    + ": запрошено " + request.quantity() + ", доступно " + inventory.getAvailableQuantity());
        }
        inventory.setReservedQuantity(inventory.getReservedQuantity() + request.quantity());
        inventoryRepository.saveAndFlush(inventory);
        return new ReserveResponse(true, inventory.getAvailableQuantity(), "Товар успешно зарезервирован");
    }

    private Inventory findInventory(Long productId) {
        return inventoryRepository.findByProductId(productId)
                .orElseThrow(() -> new NotFoundException("Остатки товара с id=" + productId + " не найдены"));
    }
}
