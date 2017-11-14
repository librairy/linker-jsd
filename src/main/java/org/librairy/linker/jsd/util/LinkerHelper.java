package org.librairy.linker.jsd.util;

import lombok.Data;
import org.librairy.boot.model.modules.EventBus;
import org.librairy.boot.storage.dao.CounterDao;
import org.librairy.boot.storage.dao.DBSessionManager;
import org.librairy.computing.cache.CacheModeHelper;
import org.librairy.computing.helper.ComputingHelper;
import org.librairy.linker.jsd.data.SimilarityDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
@Component
@Data
public class LinkerHelper {

    @Autowired
    Worker worker;

    @Autowired
    DBSessionManager dbSessionManager;

    @Autowired
    CounterDao counterDao;

    @Autowired
    SimilarityDao similarityDao;

    @Autowired
    EventBus eventBus;

    @Autowired
    ComputingHelper computingHelper;

    @Autowired
    CacheModeHelper cacheModeHelper;

    @Value("#{environment['LIBRAIRY_JSD_SCORE_MIN']?:${librairy.jsd.score.min}}")
    Double minScore;

    @Value("#{environment['LIBRAIRY_JSD_SIZE_MIN']?:${librairy.jsd.size.min}}")
    Integer minSize;

    @Value("#{environment['LIBRAIRY_JSD_SIZE_MAX']?:${librairy.jsd.size.max}}")
    Integer maxSize;

    @Value("#{environment['LIBRAIRY_JSD_THRESHOLD_MAX']?:${librairy.jsd.threshold.max}}")
    Double maxThreshold;

}
