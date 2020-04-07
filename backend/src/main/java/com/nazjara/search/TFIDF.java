package com.nazjara.search;

import com.nazjara.model.DocumentData;

import java.util.*;
import java.util.stream.Collectors;

public class TFIDF {

    public static double calculateTermFrequency(List<String> words, String term) {
        return (double) words.stream().filter(term::equalsIgnoreCase).count() / words.size();
    }

    public static DocumentData createDocumentData(List<String> words, List<String> terms) {
        DocumentData documentData = new DocumentData();

        terms.forEach(term -> documentData.putTermFrequency(term, calculateTermFrequency(words, term)));

        return documentData;
    }

    public static Map<Double, List<String>> getDocumentsSortedByScore(List<String> terms, Map<String, DocumentData> documentResults) {
        TreeMap<Double, List<String>> scoreToDocuments = new TreeMap<>();

        Map<String, Double> termToInverseDocumentFrequency = getTermToInverseDocumentFrequencyMap(terms, documentResults);

        for(String document : documentResults.keySet()) {
            DocumentData documentData = documentResults.get(document);

            double score = calculateDocumentScore(terms, documentData, termToInverseDocumentFrequency);

            addDocumentScoreToTreeMap(scoreToDocuments, score, document);
        }

        return scoreToDocuments.descendingMap();
    }

    private static void addDocumentScoreToTreeMap(TreeMap<Double, List<String>> scoreToDoc, double score, String document) {
        List<String> documentsWithCurrentScore = scoreToDoc.get(score) == null ? new ArrayList<>() : scoreToDoc.get(score);

        documentsWithCurrentScore.add(document);
        scoreToDoc.put(score, documentsWithCurrentScore);
    }

    private static double getInverseDocumentFrequency(String term, Map<String, DocumentData> documentResults) {
        double nt = documentResults.values().stream()
                .filter(documentData -> documentData.getFrequency(term) > 0.0)
                .count();

        return nt == 0 ? 0 : Math.log10(documentResults.size() / nt);
    }

    private static Map<String, Double> getTermToInverseDocumentFrequencyMap(List<String> terms, Map<String, DocumentData> documentResults) {
        return terms.stream().collect(Collectors.toMap(term -> term, term -> getInverseDocumentFrequency(term, documentResults)));
    }

    private static double calculateDocumentScore(List<String> terms, DocumentData documentData, Map<String, Double> termToInverseDocumentFrequency) {
        double score = 0;

        for (String term : terms) {
            double termFrequency = documentData.getFrequency(term);
            double inverseTermFrequency = termToInverseDocumentFrequency.get(term);
            score += termFrequency * inverseTermFrequency;
        }

        return score;
    }

    public static List<String> getWordsFromLine(String line) {
        return Arrays.asList(line.split("(\\.)+|(,)+|( )+|(-)+|(\\?)+|(!)+|(;)+|(:)+|(/d)+|(/n)+"));
    }

    public static List<String> getWordsFromLines(List<String> lines) {
        List<String> words = new ArrayList<>();

        lines.forEach(line -> words.addAll(getWordsFromLine(line)));

        return words;
    }
}