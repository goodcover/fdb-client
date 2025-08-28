package com.apple.foundationdb.record.lucene.search;

import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import org.apache.lucene.document.*;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.flexible.core.messages.QueryParserMessages;
import org.apache.lucene.search.Query;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.queryparser.flexible.standard.config.PointsConfig;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.text.NumberFormat;
import java.util.Map;

public class LuceneOptimizedMultiFieldStopWordsRangeQueryParser extends LuceneOptimizedMultiFieldStopWordsQueryParser {

    public LuceneOptimizedMultiFieldStopWordsRangeQueryParser(String[] fields, Analyzer analyzer, Map<String, PointsConfig> pointsConfig, CharArraySet stopWords) {
        super(fields, analyzer, pointsConfig, stopWords);
    }

    /**
     * Parses a default numeric value based on the provided PointsConfig and part string.
     *
     * @param cfg  the PointsConfig that specifies the type and number format
     * @param part the string representation of the number to parse, or null to use default values
     * @param low  a Boolean indicating whether to use the minimum or maximum value for the type if part is null
     * @return the parsed Number, or a default value if part is null
     * @throws ParseException if the type is unsupported or the part cannot be parsed
     */
    private Number parseDefault(PointsConfig cfg, @Nullable String part, Boolean low) throws ParseException {
        if (part == null) {
            if (Integer.class.equals(cfg.getType())) {
                return low ? Integer.MIN_VALUE : Integer.MAX_VALUE;
            } else if (Long.class.equals(cfg.getType())) {
                return low ? Long.MIN_VALUE : Long.MAX_VALUE;
            } else if (Double.class.equals(cfg.getType())) {
                return low ? Double.NEGATIVE_INFINITY : Double.POSITIVE_INFINITY;
            } else if (Float.class.equals(cfg.getType())) {
                return low ? Float.NEGATIVE_INFINITY : Float.POSITIVE_INFINITY;
            } else {
                throw new ParseException(QueryParserMessages.UNSUPPORTED_NUMERIC_DATA_TYPE);
            }
        } else {
            try {
                NumberFormat format = cfg.getNumberFormat();
                return format.parse(part);
            } catch (java.text.ParseException pe) {
                throw new ParseException(QueryParserMessages.COULD_NOT_PARSE_NUMBER);
            }
        }
    }



    @Nonnull
    @Override
    public Query attemptConstructRangeQueryWithPointsConfig(String field, String part1, String part2, boolean startInclusive, boolean endInclusive) throws ParseException {
        final var pointsConfig = getPointsConfig();
        /*
         * Lucene doesn't really understand types, so unless we tell it that we are looking at numeric-valued
         * data points in our scan, it will parse everything as text, which results in incorrect results
         * being returned over range scans.
         *
         * To avoid this, we use a PointConfig map (an idea taken from Lucene's StandardAnalyzer). This allows
         * us to specify the data types for individual fields in Lucene, which we use here to create the correct
         * type of Query object for range scans.
         */
        PointsConfig cfg = pointsConfig.get(field);
        if (cfg == null) {
            return constructRangeQueryWithoutPointsConfig(field, part1, part2, startInclusive, endInclusive);
        } else if (cfg instanceof BooleanPointsConfig) {
            byte[] p1 = "true".equalsIgnoreCase(part1) ? BooleanPointsConfig.TRUE_BYTES : BooleanPointsConfig.FALSE_BYTES;
            byte[] p2 = "true".equalsIgnoreCase(part2) ? BooleanPointsConfig.TRUE_BYTES : BooleanPointsConfig.FALSE_BYTES;
            return BinaryPoint.newRangeQuery(field, p1, p2);
        } else {
            Number start;
            Number end;
            try {
                start = parseDefault(cfg, part1, true);
                end = parseDefault(cfg, part2, false);
            } catch (ParseException pe) {
                throw pe;
            }

            if (Integer.class.equals(cfg.getType())) {
                return newIntegerRangeQuery(field, startInclusive, endInclusive, start, end);
            } else if (Long.class.equals(cfg.getType())) {
                return newLongRangeQuery(field, startInclusive, endInclusive, start, end);
            } else if (Double.class.equals(cfg.getType())) {
                return newDoubleRangeQuery(field, startInclusive, endInclusive, start, end);
            } else if (Float.class.equals(cfg.getType())) {
                return newFloatRangeQuery(field, startInclusive, endInclusive, start, end);
            } else {
                throw new ParseException(QueryParserMessages.UNSUPPORTED_NUMERIC_DATA_TYPE);
            }
        }
    }

    @SpotBugsSuppressWarnings(value = "FE_FLOATING_POINT_EQUALITY", justification = "Floating point values are special sentinel values")
    @Nonnull
    private Query newFloatRangeQuery(final String field, final boolean startInclusive, final boolean endInclusive, final Number start, final Number end) throws ParseException {
        float s = start.floatValue();
        float e = end.floatValue();
        if (s > e) {
            //probably not the best error message, but it's what Lucene offers us
            throw new ParseException(QueryParserMessages.INVALID_SYNTAX);
        }

        //lucene range queries are inclusive, so adjust ranges as needed
        if (!startInclusive) {
            if (s == Float.MAX_VALUE || s == Float.POSITIVE_INFINITY) {
                return FloatPoint.newSetQuery(field);
            } else {
                s = Math.nextAfter(s, Float.MAX_VALUE);
            }
        }
        if (!endInclusive) {
            if (e == Float.MIN_VALUE || e == Float.NEGATIVE_INFINITY) {
                return FloatPoint.newSetQuery(field);
            } else {
                e = Math.nextAfter(e, -Float.MAX_VALUE);
            }
        }

        return FloatPoint.newRangeQuery(field, s, e);
    }

    @SpotBugsSuppressWarnings(value = "FE_FLOATING_POINT_EQUALITY", justification = "Floating point values are special sentinel values")
    @Nonnull
    private Query newDoubleRangeQuery(final String field, final boolean startInclusive, final boolean endInclusive, final Number start, final Number end) throws ParseException {
        double s = start.doubleValue();
        double e = end.doubleValue();

        if (s > e) {
            throw new ParseException(QueryParserMessages.INVALID_SYNTAX);
        }
        //lucene range queries are inclusive, so adjust ranges as needed
        if (!startInclusive) {
            if (s == Double.MAX_VALUE || s == Double.POSITIVE_INFINITY) {
                return DoublePoint.newSetQuery(field);
            } else {
                s = Math.nextAfter(s, Double.MAX_VALUE);
            }
        }
        if (!endInclusive) {
            if (e == Double.MIN_VALUE || e == Double.NEGATIVE_INFINITY) {
                return DoublePoint.newSetQuery(field);
            } else {
                e = Math.nextAfter(e, -Double.MAX_VALUE);
            }
        }

        return DoublePoint.newRangeQuery(field, s, e);
    }

    @Nonnull
    private Query newLongRangeQuery(final String field, final boolean startInclusive, final boolean endInclusive, final Number start, final Number end) throws ParseException {
        long s = start.longValue();
        long e = end.longValue();
        if (s > e) {
            throw new ParseException(QueryParserMessages.INVALID_SYNTAX);
        }
        /*
         * we need to adjust ranges to remove inclusive values if we need to.
         *
         * If s == Long.MAX_VALUE, then we can't increment it without potentially
         * causing an error (due to long overflows), but we know that if you are specifying
         * the range as (MAX_VALUE,...) then that is an empty set by definition, so
         * we  return a Query that will always be empty. Similarly if we have e == Long.MIN_VALUE
         * and we want to be exclusive on the end point
         */
        //lucene range queries are inclusive, so adjust ranges as needed
        if (!startInclusive) {
            if (s == Long.MAX_VALUE) {
                //does a point-in-set query but with an empty set, so should always return false.
                //there may be cheaper ways to do this in Lucene, but I'm not aware of them
                return LongPoint.newSetQuery(field);
            } else {
                s = Math.incrementExact(s);
            }
        }
        if (!endInclusive) {
            if (e == Long.MIN_VALUE) {
                return LongPoint.newSetQuery(field);
            } else {
                e = Math.decrementExact(e);
            }
        }

        return LongPoint.newRangeQuery(field, s, e);
    }

    @Nonnull
    private Query newIntegerRangeQuery(final String field, final boolean startInclusive, final boolean endInclusive, final Number start, final Number end) throws ParseException {
        int s = start.intValue();
        int e = end.intValue();
        if (s > e) {
            //probably not the best error message, but it's what Lucene offers us
            throw new ParseException(QueryParserMessages.INVALID_SYNTAX);
        }
        //lucene range queries are inclusive, so adjust ranges as needed
        if (!startInclusive) {
            if (s == Integer.MAX_VALUE) {
                return IntPoint.newSetQuery(field);
            } else {
                s = Math.addExact(s, 1);
            }
        }
        if (!endInclusive) {
            if (e == Integer.MIN_VALUE) {
                return IntPoint.newSetQuery(field);
            } else {
                e = Math.addExact(e, -1);
            }
        }

        return IntPoint.newRangeQuery(field, s, e);
    }
}
