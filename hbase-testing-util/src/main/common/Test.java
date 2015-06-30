package main.common;



/**
 * Created by wangxiaoyi on 15/6/11.
 */
public class Test {

    public static void main(String []args){

        for(int i = 0; i < 1000000; ++i){
            System.out.println(String.format("%07d", i));
        }


    }

    public static String genString(boolean throwEx){
        try{
            if(throwEx){
                throw new RuntimeException("runtime exception");
            }
            System.out.print("in try");
            return "no exception";
        }catch (Exception EX){
            System.out.println("in catch");
            return "exception";
        }finally {
            System.out.println("in finally");
            return "finally";
        }
    }
}
