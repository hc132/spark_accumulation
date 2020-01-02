/**
 * Created on 2019/9/2 16:06
 *
 * @author : HC
 */
public class StringTest {
    public static void changeStr(String str) {
        str = "welcome";
    }

    public static void main(String[] args) {
        String str = "1234";
        changeStr(str);
        System.out.println(str);
    }

}
