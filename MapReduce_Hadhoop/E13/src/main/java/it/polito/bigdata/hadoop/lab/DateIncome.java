package it.polito.bigdata.hadoop.lab;

public class DateIncome {
	private String date;
	private int income;
	
	public DateIncome(int income) {
		this.income = income;
	}
	
	public DateIncome(String date, int income) {
		this.date = date;
		this.income = income;
	}
	
	public int getIncome() {
		return this.income;
	}
	
	public void setNewValues(String date, int income) {
		this.date = date;
		this.income = income;
	}
	
	public String getDate() {
		return this.date;
	}
}
