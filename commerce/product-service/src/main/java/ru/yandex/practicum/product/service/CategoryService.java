package ru.yandex.practicum.product.service;

import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.product.dto.CategoryDto;
import ru.yandex.practicum.product.dto.CreateCategoryRequest;
import ru.yandex.practicum.product.entity.Category;
import ru.yandex.practicum.product.exception.NotFoundException;
import ru.yandex.practicum.product.mapper.CategoryMapper;
import ru.yandex.practicum.product.repository.CategoryRepository;

import java.util.List;

@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class CategoryService {

    private final CategoryRepository categoryRepository;

    public List<CategoryDto> getAll() {
        return categoryRepository.findAll(Sort.by("id")).stream()
                .map(CategoryMapper::toDto)
                .toList();
    }

    public CategoryDto getById(Long id) {
        return CategoryMapper.toDto(findCategory(id));
    }

    @Transactional
    public CategoryDto create(CreateCategoryRequest request) {
        return CategoryMapper.toDto(categoryRepository.save(CategoryMapper.toEntity(request)));
    }

    Category findCategory(Long id) {
        return categoryRepository.findById(id)
                .orElseThrow(() -> new NotFoundException("Категория с id=" + id + " не найдена"));
    }
}
