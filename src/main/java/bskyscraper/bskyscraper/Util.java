package bskyscraper.bskyscraper;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Util {

    private static void appendArrayAsQueryParams(StringBuilder sb, String paramName, String[] values, boolean isFirst) {
        if (isFirst) {
            sb.append("?");
        } else {
            sb.append("&");
        }

        boolean firstColl = true;
        for(String value : values) {
            if(firstColl) {
                firstColl = false;
            } else {
                sb.append("&");
            }
            sb.append(String.format("%s=%s", paramName, value));
        }

    }

    public static String makeJetstreamSubUrl(String[] wantedCollections, String[] wantedDids) {
        if (wantedDids == null) {
            wantedDids = new String[]{};
        }
        var urlBuilder = new StringBuilder();
        urlBuilder.append(String.format("wss://%s/subscribe", Constants.bskyJetstreamHost));

        if (wantedCollections.length > 0) {
            appendArrayAsQueryParams(urlBuilder, "wantedCollections", wantedCollections, true);
        }
        if (wantedDids.length > 0) {
            appendArrayAsQueryParams(urlBuilder, "wantedDids", wantedDids, wantedCollections.length == 0);
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
