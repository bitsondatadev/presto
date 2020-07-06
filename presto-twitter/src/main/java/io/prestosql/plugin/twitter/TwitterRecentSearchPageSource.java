package io.prestosql.plugin.twitter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.io.CharStreams;
import io.airlift.log.Logger;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.type.Type;


import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class TwitterRecentSearchPageSource implements ConnectorPageSource
{
    private static final Logger LOG = Logger.get(TwitterRecentSearchPageSource.class);

    private final TweetIterator iterator;
    private final TwitterClient client;
    private final TwitterConfig config;
    private final BlockBuilder[] columnBuilders;
    private final List<TwitterColumnHandle> columns;
    private final int[] fieldToColumnIndex;

    private BufferedReader reader;

    private long totalBytes = 0;

    public TwitterRecentSearchPageSource(
            TwitterClient client,
            TwitterConfig config,
            TwitterTableHandle table,
            List<TwitterColumnHandle> columns) {

        requireNonNull(client, "client is null");
        requireNonNull(config, "config is null");
        requireNonNull(columns, "columns is null");

        this.columns = ImmutableList.copyOf(columns);

        this.client = client;

        this.config = config;

        columnBuilders = columns.stream()
                .map(TwitterColumnHandle::getColumnType)
                .map(type -> type.createBlockBuilder(null, 1))
                .toArray(BlockBuilder[]::new);

        fieldToColumnIndex = new int[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            TwitterColumnHandle columnHandle = columns.get(i);
            fieldToColumnIndex[i] = columnHandle.getOrdinalPosition();
        }

        iterator = new TweetIterator(config);

    }

    @Override
    public long getCompletedBytes() {
        return totalBytes;
    }

    @Override
    public long getReadTimeNanos() {
        return iterator.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return !iterator.hasNext();
    }

    @Override
    public Page getNextPage() {

        if (isFinished()) {
            Page emptyPage = new Page(0);
            return emptyPage;
        }

        JsonNode tweet = iterator.next();
        System.out.println(tweet.asText());
        totalBytes += tweet.asText().getBytes().length;
        for(int i = 0; i < columns.size(); i++){
            String columnName = columns.get(i).getColumnName();
            Type columnType = columns.get(i).getColumnType();

            if(tweet.has(columnName)){
                if(columnType == BIGINT) {
                    BIGINT.writeLong(columnBuilders[i], Long.parseLong(tweet.get(columnName).asText()));
                } else if (columnType == VARCHAR){
                    //VARCHAR.writeSlice(columnBuilders[i], Slices.utf8Slice(tweet.get(columnName).asText()));
                    VARCHAR.writeString(columnBuilders[i], tweet.get(columnName).asText());
                }
            } else {
                columnBuilders[i].appendNull();
            }
        }



        Block[] blocks = new Block[columnBuilders.length];
        for (int i = 0; i < columnBuilders.length; i++) {
            blocks[i] = columnBuilders[i].build();
            columnBuilders[i] = columnBuilders[i].newBlockBuilderLike(null);
        }

        return new Page(blocks);
    }

    @Override
    public long getSystemMemoryUsage() {
        return 0;
    }

    @Override
    public void close() {

    }

    private static class TweetIterator
            extends AbstractIterator<JsonNode>
    {

        private long readTimeNanos;
        private Iterator<JsonNode> jsonNodeIterator;

        public TweetIterator(TwitterConfig config)
        {

            String accessToken = TwitterClient.getAccessToken(config.getConsumerKey(), config.getConsumerSecret());
            InputStream twitterInputStream = TwitterClient.getTwitterStream(accessToken);

            String json = null;
            long start = System.nanoTime();
            try (Reader reader = new InputStreamReader(twitterInputStream)) {
                json = CharStreams.toString(reader);
                readTimeNanos += System.nanoTime() - start;
            } catch (IOException e) {
                e.printStackTrace();
            }

            System.out.println(json);

            ObjectMapper mapper = new ObjectMapper();
            JsonNode node;
            try {
                node = mapper.readTree(json);

                if(node.has("data")){
                    jsonNodeIterator = node.get("data").elements();
                }else{
                    throw new IllegalStateException("got error back from twitter, bad call or bad credentials");
                }
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }

        public long getReadTimeNanos()
        {
            return readTimeNanos;
        }

        @Override
        protected JsonNode computeNext()
        {
            if(jsonNodeIterator.hasNext()){
                return jsonNodeIterator.next();
            } else {
                return endOfData();
            }

        }
    }
}
