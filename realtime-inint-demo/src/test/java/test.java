import sun.net.www.protocol.http.HttpURLConnection;
import java.util.Random;

public class test {
    public static void main(String[] args) {
//        int length = 2; // 生成10个汉字
//        StringBuilder sb = new StringBuilder();
//        Random rand = new Random();
//
//        for (int i = 0; i < length; i++) {
//            // 基本汉字Unicode范围：0x4E00-0x9FA5 (19968-40869)
//            char ch = (char)(0x4E00 + rand.nextInt(0x9FA5 - 0x4E00 + 1));
//            sb.append(ch);
//        }
//
//        System.out.println("随机中文: " + sb.toString());
        Random rand = new Random();
        System.out.println(rand);
    }
}
