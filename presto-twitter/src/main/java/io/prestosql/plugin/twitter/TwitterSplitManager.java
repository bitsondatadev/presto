/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.plugin.twitter;

import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.connector.ConnectorSplitSource;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.FixedSplitSource;
import io.prestosql.spi.connector.TableNotFoundException;

import javax.inject.Inject;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import org.apache.http.client.utils.URIBuilder;

import static java.util.Objects.requireNonNull;

public class TwitterSplitManager
        implements ConnectorSplitManager
{
    private final TwitterConfig twitterConfig;

    @Inject
    public TwitterSplitManager(TwitterConfig twitterConfig)
    {
        this.twitterConfig = requireNonNull(twitterConfig, "config is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle connectorTableHandle,
            SplitSchedulingStrategy splitSchedulingStrategy)
    {
        TwitterTableHandle tableHandle = (TwitterTableHandle) connectorTableHandle;

        // this can happen if table is removed during a query
        if (tableHandle == null) {
            throw new TableNotFoundException(tableHandle.toSchemaTableName());
        }



        /*List<ConnectorSplit> splits = new ArrayList<>();
        for (URI uri : table.getSources()) {
            splits.add(new TwitterSplit(uri));
        }
        Collections.shuffle(splits);*/
        URI uri = null;
        try{
            uri = new URIBuilder(TwitterClient.SEARCH_TWEET_URI).build();
        }catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }

        return new FixedSplitSource(List.of(new TwitterSplit(uri)));
    }
}
