package ai.spark.spark.m1.dto;

public class IntegerWithSqRoot {

    private int intNum;
    private double sqRoot;

    public IntegerWithSqRoot(int intNum) {
        this.intNum = intNum;
        this.sqRoot = Math.sqrt(intNum);
    }

    public double getSqRoot() {
        return sqRoot;
    }

    public void setSqRoot(double sqRoot) {
        this.sqRoot = sqRoot;
    }

    public int getIntNum() {
        return intNum;
    }

    public void setIntNum(int intNum) {
        this.intNum = intNum;
    }
}
