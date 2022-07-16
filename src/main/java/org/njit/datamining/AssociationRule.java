package org.njit.datamining;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AssociationRule {

	public List<FrequentItemset> calculateAssociation(Map<Integer, List<Set<String>>> filteredMap, Map<Set<String>, Integer> supportMap, double userConfidence) {
		try {
			List<FrequentItemset> rule = new ArrayList<>();
			for(int key : filteredMap.keySet()) {
				if(key<2) {
					continue;
				}
				for(Set<String> set : filteredMap.get(key)) {
					List<List<String>> subsetLists = generateSubsets(new ArrayList<>(set),userConfidence);

					List<Set<String>> subsets = new ArrayList<>();
					for(List<String> l : subsetLists) {
						Set<String> k = new HashSet<>(l);
						subsets.add(k);
					}
					calculateFinalAssociations(rule, subsets, supportMap, set, userConfidence);
				}
			}
			return rule;
		} catch (Exception e) {
			System.out.println("Exception in calculateAssociation : "+e.getMessage());
			throw e;
		}
	}

	private void calculateFinalAssociations(List<FrequentItemset> result, List<Set<String>> subsetsList, Map<Set<String>, Integer> supportMap, Set<String> set, double userConfidence) {
		try {
			for(int i=0; i<subsetsList.size(); i++) {
				for(int j=i+1; j<subsetsList.size(); j++) {
					if(subsetsList.get(i).size()+subsetsList.get(j).size()!=set.size()) continue;

					boolean stop= false;
					for (String value : subsetsList.get(i)) {
						if (subsetsList.get(j).contains(value)) {
							stop = true;
							break;
						}
					}
					if(stop) continue;
					double conf = ((double) supportMap.get(set) / supportMap.get(subsetsList.get(i))) * 100;
					if(conf >= userConfidence) {
						FrequentItemset rule1 = new FrequentItemset();
						rule1.setAntecedent(subsetsList.get(i));
						rule1.setConsequent(subsetsList.get(j));
						rule1.setUserConfidence(conf);
						rule1.setUserSupport(supportMap.get(set));
						result.add(rule1);
					}
					conf = ((double) supportMap.get(set) / supportMap.get(subsetsList.get(j))) * 100;
					if(conf >=userConfidence) {
						FrequentItemset rule2 = new FrequentItemset();
						rule2.setAntecedent(subsetsList.get(j));
						rule2.setConsequent(subsetsList.get(i));
						rule2.setUserSupport(supportMap.get(set));
						rule2.setUserConfidence(conf);
						result.add(rule2);
					}
				}
			}
		} catch (Exception e) {
			System.out.println("Exception in calculateFinalAssociations : "+e.getMessage());
			throw e;
		}
	}
	private List<List<String>> generateSubsets(List<String> list, double userConfidence) {
		try {
			List<List<String>> subset = new ArrayList<>();
			generateAllSubsets(subset, new LinkedList<>(), list, 0,userConfidence);
			return subset;
		} catch (Exception e) {
			System.out.println("Exception in generateSubsets : "+e.getMessage());
			throw e;
		}
	}

	private void generateAllSubsets(List<List<String>> subset, LinkedList<String> currentSubset, List<String> subsetList, int s, double userConfidence) {
		try {
			if(currentSubset.size()!=0 && currentSubset.size()!=subsetList.size()) {
				subset.add(new ArrayList<>(currentSubset));
			}
			for(int i=s; i<subsetList.size(); i++) {
				currentSubset.addLast(subsetList.get(i));
				generateAllSubsets(subset, currentSubset, subsetList, i+1,userConfidence);
				currentSubset.removeLast();
			}
		} catch (Exception e) {
			System.out.println("Exception in generateAllSubsets : "+e.getMessage());
			throw e;
		}
	}
}