import com.libo.dto.Customer;
import com.libo.my.MyElasticSearch;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

/**
 * @FileName: myTest
 * @author: bli
 * @date: 2020年02月20日 10:25
 * @description:
 */
public class myTest {

    @Test
    public void testIndex() throws IOException {
        MyElasticSearch my = new MyElasticSearch();
        List list = new ArrayList();
        Customer customer  = new Customer();
        customer.setId(String.valueOf(12));
        customer.setName("ARTENS玻璃");
        list.add(customer);
        my.bulkCreateIndex(list, Customer.class);
    }

    @Test
    public void testSearch() throws IOException, InvocationTargetException, IllegalAccessException, InstantiationException {
        MyElasticSearch my = new MyElasticSearch();
        Class<?> obj = Customer.class;
        my.search("name","ARTENS地板", 0, 5, Customer.class);
    }
}
