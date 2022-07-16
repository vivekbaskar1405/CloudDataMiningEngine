package org.njit.datamining;
import java.util.HashSet;
import java.util.Set;

public class FrequentItemset {

	private Set<String> antecedent = new HashSet<>();
	private Set<String> consequent = new HashSet<>();
	private double userConfidence;
	private double userSupport;

	public Set<String> getAntecedent() {
		return antecedent;
	}

	public void setAntecedent(Set<String> antecedent) {
		this.antecedent = antecedent;
	}


	public Set<String> getConsequent() {
		return consequent;
	}

	public void setConsequent(Set<String> consequent) {
		this.consequent = consequent;
	}

	public double getUserConfidence() {
		return userConfidence;
	}

	public void setUserConfidence(double userConfidence) {
		this.userConfidence = userConfidence;
	}

	public double getUserSupport() {
		return userSupport;
	}

	public void setUserSupport(double userSupport) {
		this.userSupport = userSupport;
	}


}
