package p1;

import java.util.Scanner;

public class Client {


    public static void main(String[] args) throws Exception {
        Scanner myObj = new Scanner(System.in);  // Create a Scanner object
        System.out.println("Choose Program 1~4");

        String program = myObj.nextLine();  // Read user input
        System.out.println("Program: " + program);  // Output user input
        switch (program){
            case "1":
                //System.out.println("1");
                commonFriend1 cf = new commonFriend1();
                cf.main(args);
                break;
            case "2":
                //System.out.println("2");
                topTen tt = new topTen();
                tt.main(args);
                break;
            case "3":
                //System.out.println("3");
                friendBirth sf = new friendBirth();
                sf.main(args);
                break;
            case "4":
                //System.out.println("4");
                topAge ta = new topAge();
                ta.main(args);
                break;
            default:
                System.out.println("wrong number");
                break;
        }
    }
}
