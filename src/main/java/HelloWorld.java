import org.codehaus.jackson.map.util.ISO8601Utils;

import java.io.IOException;

/**
 * @author masai
 * @date 2021/3/5
 */
public class HelloWorld {
    public static void main(String[] args) {
        System.out.println(0.3-0.2);
        RuntimeException e = new RuntimeException();
        String tmp = "abcaaaaaaa";
        String tmp1 = tmp.replace('a','b');
        CharSequence abc = "abc";
        String tmp2 = tmp1.replaceAll("bb","w");
        System.out.println(tmp1);
        System.out.println(tmp2);
    }
}
