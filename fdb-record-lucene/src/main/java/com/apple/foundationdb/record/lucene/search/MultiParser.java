package com.apple.foundationdb.record.lucene.search;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.queryparser.flexible.standard.config.PointsConfig;

import javax.annotation.Nonnull;
import com.google.auto.service.AutoService;
import java.util.Map;

@AutoService(LuceneQueryParserFactory.class)
public class MultiParser implements LuceneQueryParserFactory {
    @Override
    public QueryParser createQueryParser(final String field, final Analyzer analyzer, @Nonnull final Map<String, PointsConfig> pointsConfig) {
        return new LuceneOptimizedStopWordsQueryParser(field, analyzer, pointsConfig, getStopWords());
    }

    @Nonnull
    @Override
    public QueryParser createMultiFieldQueryParser(String[] fields, Analyzer analyzer, @Nonnull final Map<String, PointsConfig> pointsConfig) {
        QueryParser parser = new LuceneOptimizedMultiFieldStopWordsRangeQueryParser(fields, analyzer, pointsConfig, getStopWords());
        parser.setDefaultOperator(QueryParser.Operator.OR);
        return parser;
    }

    @Nonnull
    protected CharArraySet getStopWords() {
        return EnglishAnalyzer.ENGLISH_STOP_WORDS_SET;
    }
}
