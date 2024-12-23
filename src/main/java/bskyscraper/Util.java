package bskyscraper;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Util {

    public static String makeJetstreamSubUrl(String[] wantedCollections) {
        var urlBuilder = new StringBuilder();
        urlBuilder.append(String.format("wss://%s/subscribe", Constants.bskyJetstreamHost));

        if (wantedCollections.length > 0) {
            urlBuilder.append("?");
            boolean firstColl = true;
            for(String coll : wantedCollections) {
                if(firstColl) {
                    firstColl = false;
                } else {
                    urlBuilder.append("&");
                }
                urlBuilder.append(String.format("wantedCollections=%s", coll));
            }
        }
        return urlBuilder.toString();
    }

    public static boolean keepMessage(String message) {
        String regex = """
            "kind":\\s*"(.*?)" """;

        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(message);
        if (matcher.find()) {
            String kind = matcher.group(1);
            if(kind.equals("commit")) {
                return true;
            }
        }
        return false;
    }
    
}
