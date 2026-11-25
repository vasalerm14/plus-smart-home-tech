package ru.yandex.practicum.order.service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.order.dto.CreateOrderRequest;
import ru.yandex.practicum.order.dto.OrderDto;
import ru.yandex.practicum.order.entity.Order;
import ru.yandex.practicum.order.entity.OrderItem;
import ru.yandex.practicum.order.entity.OrderStatus;
import ru.yandex.practicum.order.exception.NotFoundException;
import ru.yandex.practicum.order.mapper.OrderMapper;
import ru.yandex.practicum.order.repository.OrderRepository;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class OrderService {

    private final OrderRepository orderRepository;

    @Transactional
    public OrderDto create(CreateOrderRequest request) {
        Order order = new Order();
        order.setCustomerName(request.customerName());
        order.setCustomerEmail(request.customerEmail());
        order.setStatus(OrderStatus.CREATED);
        order.setCreatedAt(LocalDateTime.now());
        request.items().stream()
                .map(OrderMapper::toItemEntity)
                .forEach(order::addItem);
        order.setTotalPrice(calculateTotal(order.getItems()));
        return OrderMapper.toDto(orderRepository.save(order));
    }

    public OrderDto getById(Long id) {
        return orderRepository.findWithItemsById(id)
                .map(OrderMapper::toDto)
                .orElseThrow(() -> new NotFoundException("Заказ с id=" + id + " не найден"));
    }

    public List<OrderDto> getAll() {
        return toDtos(orderRepository.findAllByOrderById());
    }

    public List<OrderDto> getByEmail(String email) {
        return toDtos(orderRepository.findAllByCustomerEmailOrderById(email.trim()));
    }

    private BigDecimal calculateTotal(List<OrderItem> items) {
        return items.stream()
                .map(item -> item.getPrice().multiply(BigDecimal.valueOf(item.getQuantity())))
                .reduce(BigDecimal.ZERO, BigDecimal::add);
    }

    private List<OrderDto> toDtos(List<Order> orders) {
        return orders.stream()
                .map(OrderMapper::toDto)
                .toList();
    }
}
