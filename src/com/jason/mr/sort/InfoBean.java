package com.jason.mr.sort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * <b><code>InfoBean</code></b>
 * <p>
 * class_comment
 * <p>
 * <b>Creation Time:</b> 2018/1/4 11:45.
 *
 * @author yangjiangshui
 * @version ${Revision} 2018/1/4
 * @since spring-boot project_version
 */
public class InfoBean implements WritableComparable<InfoBean> {
    private String account;
    private double income;
    private double expenses;
    private double surplus;

    public void set(String account, double income, double expenses) {
        this.account = account;
        this.income = income;
        this.expenses = expenses;
        this.surplus = income - expenses;
    }

    @Override
    public int compareTo(InfoBean o) {
        if (this.income == o.getIncome()) {
            return this.expenses > o.getExpenses() ? 1 : -1;
        } else {
            return this.income > o.getIncome() ? -1 : 1;
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(account);
        out.writeDouble(income);
        out.writeDouble(expenses);
        out.writeDouble(surplus);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.account = in.readUTF();
        this.income = in.readDouble();
        this.expenses = in.readDouble();
        this.surplus = in.readDouble();
    }

    public String getAccount() {
        return account;
    }

    public void setAccount(String account) {
        this.account = account;
    }

    public double getIncome() {
        return income;
    }

    public void setIncome(double income) {
        this.income = income;
    }

    public double getExpenses() {
        return expenses;
    }

    public void setExpenses(double expenses) {
        this.expenses = expenses;
    }

    public double getSurplus() {
        return surplus;
    }

    public void setSurplus(double surplus) {
        this.surplus = surplus;
    }

    @Override
    public String toString() {
        return this.income +
                "\t" + this.expenses +
                "\t" + surplus;
    }
}
