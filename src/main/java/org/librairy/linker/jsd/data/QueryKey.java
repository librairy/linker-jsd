package org.librairy.linker.jsd.data;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Optional;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
@Data
@EqualsAndHashCode
public class QueryKey {

    String domainUri;
    Integer size;
    Optional<Long> offset;

}
