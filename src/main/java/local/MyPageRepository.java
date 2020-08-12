package local;

import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;

import java.util.List;

public interface MyPageRepository extends CrudRepository<MyPage, Long> {

    List<MyPage> findByOrderId(Long orderId);

        void deleteByOrderId(Long orderId);
        void deleteByDeliveryId(Long deliveryId);
}