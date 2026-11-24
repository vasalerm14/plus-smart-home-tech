package ru.yandex.practicum.product.service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.product.dto.CreateProductRequest;
import ru.yandex.practicum.product.dto.ProductDto;
import ru.yandex.practicum.product.dto.UpdateProductRequest;
import ru.yandex.practicum.product.entity.Category;
import ru.yandex.practicum.product.entity.Product;
import ru.yandex.practicum.product.exception.NotFoundException;
import ru.yandex.practicum.product.mapper.ProductMapper;
import ru.yandex.practicum.product.repository.ProductRepository;

import java.util.List;

@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class ProductService {

    private final ProductRepository productRepository;
    private final CategoryService categoryService;

    public List<ProductDto> getAllActive() {
        return toDtos(productRepository.findAllByActiveTrueOrderById());
    }

    public ProductDto getById(Long id) {
        return ProductMapper.toDto(findProduct(id));
    }

    public List<ProductDto> getByCategory(Long categoryId) {
        categoryService.findCategory(categoryId);
        return toDtos(productRepository.findAllByCategoryIdAndActiveTrueOrderById(categoryId));
    }

    public List<ProductDto> search(String query) {
        return toDtos(productRepository.findAllByNameContainingIgnoreCaseAndActiveTrueOrderById(query.trim()));
    }

    @Transactional
    public ProductDto create(CreateProductRequest request) {
        Category category = request.categoryId() == null ? null : categoryService.findCategory(request.categoryId());
        return ProductMapper.toDto(productRepository.save(ProductMapper.toEntity(request, category)));
    }

    @Transactional
    public ProductDto update(Long id, UpdateProductRequest request) {
        Product product = findProduct(id);
        if (request.name() != null) {
            product.setName(request.name());
        }
        if (request.description() != null) {
            product.setDescription(request.description());
        }
        if (request.price() != null) {
            product.setPrice(request.price());
        }
        if (request.categoryId() != null) {
            product.setCategory(categoryService.findCategory(request.categoryId()));
        }
        if (request.imageUrl() != null) {
            product.setImageUrl(request.imageUrl());
        }
        if (request.active() != null) {
            product.setActive(request.active());
        }
        return ProductMapper.toDto(product);
    }

    private Product findProduct(Long id) {
        return productRepository.findWithCategoryById(id)
                .orElseThrow(() -> new NotFoundException("Товар с id=" + id + " не найден"));
    }

    private List<ProductDto> toDtos(List<Product> products) {
        return products.stream()
                .map(ProductMapper::toDto)
                .toList();
    }
}
