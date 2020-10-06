import java.util.Random;

/**
 * Created by jxy on 2020/10/1 14:01
 */
public class TestRandom {
    public static void main(String[] args) {
        Random random =new Random(10);
        for(int i=0;i<10;i++){
            System.out.println(random.nextInt());
        }
        System.out.println("*******************************");
        Random random1 =new Random(10);
        for(int i=0;i<10;i++){
            System.out.println(random1.nextInt());
        }
        double random2 = Math.random();
    }
}
