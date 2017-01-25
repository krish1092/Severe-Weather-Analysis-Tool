package edu.iastate.geol.meteor.swat.analytics.bean;

public class OverallResult {
	
	
	private String classification;
	private long classificationCount,hailBelow1,hailAbove1,HailAbove2,
	thunderstormWindAbove65,thunderstormWindBelow65,flashfloodCount,
	EF0,EF1,EF2,EF3,EF4,EF5,
	nullClassificationCount;
	public String getClassification() {
		return classification;
	}
	public void setClassification(String classification) {
		this.classification = classification;
	}
	public long getClassificationCount() {
		return classificationCount;
	}
	public void setClassificationCount(long classificationCount) {
		this.classificationCount = classificationCount;
	}
	public long getHailBelow1() {
		return hailBelow1;
	}
	public void setHailBelow1(long hailBelow1) {
		this.hailBelow1 = hailBelow1;
	}
	public long getHailAbove1() {
		return hailAbove1;
	}
	public void setHailAbove1(long hailAbove1) {
		this.hailAbove1 = hailAbove1;
	}
	public long getHailAbove2() {
		return HailAbove2;
	}
	public void setHailAbove2(long hailAbove2) {
		HailAbove2 = hailAbove2;
	}
	public long getThunderstormWindAbove65() {
		return thunderstormWindAbove65;
	}
	public void setThunderstormWindAbove65(long thunderstormWindAbove65) {
		this.thunderstormWindAbove65 = thunderstormWindAbove65;
	}
	public long getThunderstormWindBelow65() {
		return thunderstormWindBelow65;
	}
	public void setThunderstormWindBelow65(long thunderstormWindBelow65) {
		this.thunderstormWindBelow65 = thunderstormWindBelow65;
	}
	public long getFlashfloodCount() {
		return flashfloodCount;
	}
	public void setFlashfloodCount(long flashfloodCount) {
		this.flashfloodCount = flashfloodCount;
	}
	public long getEF0() {
		return EF0;
	}
	public void setEF0(long eF0) {
		EF0 = eF0;
	}
	public long getEF1() {
		return EF1;
	}
	public void setEF1(long eF1) {
		EF1 = eF1;
	}
	public long getEF2() {
		return EF2;
	}
	public void setEF2(long eF2) {
		EF2 = eF2;
	}
	public long getEF3() {
		return EF3;
	}
	public void setEF3(long eF3) {
		EF3 = eF3;
	}
	public long getEF4() {
		return EF4;
	}
	public void setEF4(long eF4) {
		EF4 = eF4;
	}
	public long getEF5() {
		return EF5;
	}
	public void setEF5(long eF5) {
		EF5 = eF5;
	}
	public long getNullClassificationCount() {
		return nullClassificationCount;
	}
	public void setNullClassificationCount(long nullClassificationCount) {
		this.nullClassificationCount = nullClassificationCount;
	}
	
	

}
