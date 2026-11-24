package ru.yandex.practicum.product.repository;

import org.springframework.data.jpa.repository.EntityGraph;
import org.springframework.data.jpa.repository.JpaRepository;
import ru.yandex.practicum.product.entity.Product;

import java.util.List;
import java.util.Optional;

public interface ProductRepository extends JpaRepository<Product, Long> {

    @EntityGraph(attributePaths = "category")
    Optional<Product> findWithCategoryById(Long id);

    @EntityGraph(attributePaths = "category")
    List<Product> findAllByActiveTrueOrderById();

    @EntityGraph(attributePaths = "category")
    List<Product> findAllByCategoryIdAndActiveTrueOrderById(Long categoryId);

    @EntityGraph(attributePaths = "category")
    List<Product> findAllByNameContainingIgnoreCaseAndActiveTrueOrderById(String query);
}
