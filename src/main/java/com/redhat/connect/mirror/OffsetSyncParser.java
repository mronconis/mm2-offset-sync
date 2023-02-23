package com.redhat.connect.mirror;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.redhat.connect.mirror.OffsetSyncSerDer.*;

public class OffsetSyncParser {
    
    private final static Pattern PATTERN = Pattern.compile("topicPartition=([^,]+), upstreamOffset=([0-9]+), downstreamOffset=([0-9]+)");

    public static OffsetSync parse(String line) {

        Matcher matcher = PATTERN.matcher(line);

        if (!matcher.find() || matcher.groupCount() < 3)
            return null;

        String topic = matcher.group(1).substring(0, matcher.group(1).lastIndexOf("-"));
        int partition = Integer.parseInt(matcher.group(1).substring(matcher.group(1).lastIndexOf("-") + 1));
        
        long upstreamOffset = Long.valueOf(matcher.group(2));
        long downstreamOffset = Long.valueOf(matcher.group(3));
        
        return new OffsetSync(new TopicPartition(topic, partition), upstreamOffset, downstreamOffset);
    }
}
