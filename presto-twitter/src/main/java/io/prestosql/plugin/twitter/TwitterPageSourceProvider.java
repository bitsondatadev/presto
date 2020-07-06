package io.prestosql.plugin.twitter;

import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.ConnectorPageSourceProvider;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.DynamicFilter;

import javax.inject.Inject;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class TwitterPageSourceProvider implements ConnectorPageSourceProvider
{
        private final TwitterClient client;
        private final TwitterConfig config;

        @Inject
        public TwitterPageSourceProvider(TwitterClient client, TwitterConfig config)
        {
            this.client = requireNonNull(client, "client is null");
            this.config = requireNonNull(config, "config is null");
        }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter)
    {
        // By default, poll dynamic filtering without blocking for collection to complete.
        return new TwitterRecentSearchPageSource(client, config, (TwitterTableHandle) table,
                columns.stream()
                        .map(TwitterColumnHandle.class::cast)
                        .collect(toImmutableList()));
    }

}
