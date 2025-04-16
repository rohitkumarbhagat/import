package org.datacommons;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Base64;
import java.util.zip.GZIPInputStream;
import org.datacommons.proto.CacheData.PagedEntities;

public class Main {
  public static void main(String[] args) throws IOException {
    String value="H4sIAAAAAAAAAOMyVhJKSdZPSixO1XcCEsHJGam5iVqyAYlFicWZJZnJCol5KQplqckl+UUKKZnFqUA1xYIMYPDBHgBWdoo1PgAAAA==";
    PagedEntities elist =
        PagedEntities.parseFrom(
            new GZIPInputStream(
                new ByteArrayInputStream(Base64.getDecoder().decode(value))));
    System.out.println(elist);

  }

}
