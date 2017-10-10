package org.apache.bookkeeper.proto;

import java.util.UUID;
import com.google.common.hash.Hashing;

public class WriterId implements Comparable<WriterId> {
    public static final WriterId MAX_WRITER = new WriterId(Long.MAX_VALUE,
                                                           new UUID(Long.MAX_VALUE,
                                                                    Long.MAX_VALUE));
    final long epoch;
    final UUID unique;

    public WriterId(long epoch, UUID unique) {
        this.epoch = epoch;
        this.unique = unique;
    }

    @Override
    public int compareTo(WriterId other) {
        if (epoch < other.epoch) {
            return -1;
        } else if (epoch > other.epoch) {
            return 1;
        } else {
            return unique.compareTo(other.unique);
        }
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof WriterId) {
            WriterId otherWriter = (WriterId)other;
            return otherWriter.epoch == epoch
                && otherWriter.unique.equals(unique);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Hashing.goodFastHash(32).newHasher()
            .putLong(epoch)
            .putLong(unique.getMostSignificantBits())
            .putLong(unique.getLeastSignificantBits()).hash().asInt();
    }

    @Override
    public String toString() {
        return String.format("Writer(epoch:%d,unique:%s)", epoch, unique);
    }

    public WriterId surpass(WriterId other) {
        return new WriterId(other.epoch + 1, unique);
    }

    public void toProtobuf(BookkeeperProtocol.WriterId.Builder builder) {
        builder.setUniqueMsb(unique.getMostSignificantBits())
            .setUniqueLsb(unique.getLeastSignificantBits())
            .setEpoch(epoch);
    }

    public static WriterId fromProtobuf(BookkeeperProtocol.WriterId protobuf) {
        return new WriterId(protobuf.getEpoch(),
                            new UUID(protobuf.getUniqueMsb(),
                                     protobuf.getUniqueLsb()));
    }
}
