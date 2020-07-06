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

import io.airlift.json.ObjectMapperProvider;
import okhttp3.*;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

public class TwitterClient
{
     //https://developer.twitter.com/en/portal/dashboard
    //OATH 2 and Bearer Token
    //https://developer.twitter.com/en/docs/basics/authentication/api-reference/token
    public static final String OATH2_TOKEN_URI = "https://api.twitter.com/oauth2/token";

    //https://developer.twitter.com/en/docs/basics/authentication/oauth-2-0
    //https://developer.twitter.com/en/docs/basics/authentication/oauth-2-0/application-only

    //Twitter API
    // https://developer.twitter.com/en/docs/twitter-api/tweets/search/api-reference/get-tweets-search-recent
    public static final String SEARCH_TWEET_URI = "https://api.twitter.com/2/tweets/search/recent";




    //
    // This method calls the filtered stream endpoint and streams Tweets from it
    //
    public static InputStream getTwitterStream(String accessToken)
    {
        Request request = new Request.Builder()
                .header("Authorization", String.format("Bearer %s", accessToken))
                .url(TwitterClient.SEARCH_TWEET_URI + "?query=python&tweet.fields=created_at,source,lang,geo&expansions=author_id&user.fields=username")
                .get()
                .build();

        InputStream twitterStream;
        try {
            Response response = new OkHttpClient().newCall(request).execute();
            twitterStream = response.body().byteStream();
        } catch (IOException e) {
            twitterStream = null;
        }

        return twitterStream;
    }

    //
    // Helper method that generates bearer token by calling the /oauth2/token endpoint
    //
    public static String getAccessToken(String consumerKey, String consumerSecret)
    {

        RequestBody formBody = new FormBody.Builder()
                .add("grant_type", "client_credentials")
                .build();

        Request request = new Request.Builder()
                .header("Authorization", String.format("Basic %s", getBase64EncodedString(consumerKey, consumerSecret)))
                .url(TwitterClient.OATH2_TOKEN_URI)
                .post(formBody)
                .build();
        try {
            Response response = new OkHttpClient().newCall(request).execute();
            Map<String, Object> jsonMap = new ObjectMapperProvider().get().readValue(response.body().byteStream(), Map.class);
            return jsonMap.get("access_token").toString();
        } catch (IOException e) {
        }

        return null;
    }

    //
    // Helper method that generates the Base64 encoded string to be used to obtain bearer token
    //
    private static String getBase64EncodedString(String consumerKey, String consumerSecret)
    {
        String s = String.format("%s:%s", consumerKey, consumerSecret);
        return Base64.getEncoder().encodeToString(s.getBytes(StandardCharsets.UTF_8));
    }

}
