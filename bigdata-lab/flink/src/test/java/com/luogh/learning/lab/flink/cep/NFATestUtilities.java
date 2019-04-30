package com.luogh.learning.lab.flink.cep;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.apache.flink.cep.nfa.NFA;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.junit.Assert;

/**
 * Base method for IT tests of {@link NFA}. It provides utility methods.
 */
public class NFATestUtilities {

	public static List<List<Event>> feedNFA(List<StreamRecord<Event>> inputEvents, NFA<Event> nfa) {
		return feedNFA(inputEvents, nfa, AfterMatchSkipStrategy.noSkip());
	}

	public static List<List<Event>> feedNFA(List<StreamRecord<Event>> inputEvents, NFA<Event> nfa,
											AfterMatchSkipStrategy afterMatchSkipStrategy) {
		List<List<Event>> resultingPatterns = new ArrayList<>();

		for (StreamRecord<Event> inputEvent : inputEvents) {
			Collection<Map<String, List<Event>>> patterns = null;
//					nfa.process(
//				inputEvent.getValue(),
//				inputEvent.getTimestamp(),
//				afterMatchSkipStrategy).f0;

			for (Map<String, List<Event>> p: patterns) {
				List<Event> res = new ArrayList<>();
				for (List<Event> le: p.values()) {
					res.addAll(le);
				}
				resultingPatterns.add(res);
			}
		}
		return resultingPatterns;
	}

	public static void compareMaps(List<List<Event>> actual, List<List<Event>> expected) {
		Assert.assertEquals(expected.size(), actual.size());

		for (List<Event> p: actual) {
			Collections.sort(p, new EventComparator());
		}

		for (List<Event> p: expected) {
			Collections.sort(p, new EventComparator());
		}

		Collections.sort(actual, new ListEventComparator());
		Collections.sort(expected, new ListEventComparator());
		Assert.assertArrayEquals(expected.toArray(), actual.toArray());
	}

	private static class ListEventComparator implements Comparator<List<Event>> {

		@Override
		public int compare(List<Event> o1, List<Event> o2) {
			int sizeComp = Integer.compare(o1.size(), o2.size());
			if (sizeComp == 0) {
				EventComparator comp = new EventComparator();
				for (int i = 0; i < o1.size(); i++) {
					int eventComp = comp.compare(o1.get(i), o2.get(i));
					if (eventComp != 0) {
						return eventComp;
					}
				}
				return 0;
			} else {
				return sizeComp;
			}
		}
	}

	private static class EventComparator implements Comparator<Event> {

		@Override
		public int compare(Event o1, Event o2) {
			int nameComp = o1.getName().compareTo(o2.getName());
			int priceComp = Double.compare(o1.getPrice(), o2.getPrice());
			int idComp = Integer.compare(o1.getId(), o2.getId());
			if (nameComp == 0) {
				if (priceComp == 0) {
					return idComp;
				} else {
					return priceComp;
				}
			} else {
				return nameComp;
			}
		}
	}

	private NFATestUtilities() {
	}
}
