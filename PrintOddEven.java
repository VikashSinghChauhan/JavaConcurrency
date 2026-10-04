package com.example.demo;

/**
 * Implementation choices (pick one):
 *
 * synchronized + wait()/notify() with a boolean oddTurn flag
 * ReentrantLock + two Condition objects
 * Semaphore pair — one for odd, one for even
 * Follow-up questions an interviewer might ask:
 *
 * What if we extend this to 3 threads printing numbers divisible by 1, 2, 3?
 * What happens if you use notify() vs notifyAll() here? (hint: with only 2 threads, notify() is actually fine)
 * Can you do this without any shared mutable state? (hint: SynchronousQueue or Exchanger)
 */

public class PrintOddEven {
    Integer n;
    volatile Integer val;

    public PrintOddEven( Integer n) {
        this.n = n;
        this.val = 1;
    }

    synchronized void PrintOne() throws InterruptedException {

        while(val<=n)
        {
            while(val%3!=1)
            {
                wait();
            }
            if(val > n)break;
            System.out.println(val);
            val++;
            notifyAll();
        }

    }

    synchronized void printTwo() throws InterruptedException{

        while(val<=n){
            while(val%3!=2)
            {
                wait();
            }
            if(val > n)break;
            System.out.println(val);
            val++;
            notifyAll();
        }

    }

    synchronized void printThree() throws InterruptedException{

        while(val<=n){
            while(val%3!=0)
            {
                wait();
            }
            if(val > n)break;
            System.out.println(val);
            val++;
            notifyAll();
        }

    }

    public static void main(String[] args) {
        PrintOddEven printOddEven = new PrintOddEven(100);
        Thread t1 = new Thread(()->{
            try{
                printOddEven.PrintOne();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        Thread t2 = new Thread(() -> {
            try{
                printOddEven.printTwo();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        Thread t3 = new Thread(()->{
            try{
                printOddEven.printThree();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        t1.start();
        t2.start();
        t3.start();
    }
}
