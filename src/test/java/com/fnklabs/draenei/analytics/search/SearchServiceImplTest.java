package com.fnklabs.draenei.analytics.search;

import com.fnklabs.draenei.IgniteTest;
import com.fnklabs.draenei.MetricsFactoryImpl;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.ClusterGroup;
import org.junit.*;

import java.net.UnknownHostException;
import java.security.SecureRandom;
import java.util.List;

public class SearchServiceImplTest {

    private static final int MAX_DOCUMENTS = 40000;
    private static final String[] WORDS = {
            "принимая", "внимание", "искусственность", "границ", "элементарной", "почвы", "и", "произвольность", "ее", "положения", "в", "пространстве", "почвенного", "покрова,",
            "банкротство", "практически", "индуцирует", "модальный", "апогей.", "модальное", "письмо", "может", "быть", "реализовано", "на", "основе", "принципов", "центропостоянности",
            "и", "центропеременности,", "таким", "образом", "удобрение", "изменяет", "неизменный", "параллакс.", "пустыня", "отражает", "близкий", "дольник.", "обычная", "литература,",
            "перенесенная", "в", "сеть,", "не", "является", "сетературой", "в", "смысле", "отдельного", "жанра,", "однако", "газопылевое", "облако", "философски", "отталкивает",
            "однокомпонентный", "кайнозой.", "многочисленные", "расчеты", "предсказывают,", "а", "эксперименты", "подтверждают,", "что", "поле", "направлений", "необходимо", "и",
            "достаточно"
    };
    private SearchService searchService;
    private Ignite ignite;

    @Before
    public void setUp() throws Exception {
        ignite = Ignition.start(IgniteTest.getIgniteConfiguration());
        Ignition.start(IgniteTest.getIgniteConfiguration());

        ClusterGroup clusterGroup = ignite.cluster().forServers();

        ignite.services(clusterGroup)
              .deployMultiple(SearchServiceImpl.SERVICE_NAME, new SearchServiceImpl(), 3, 1);

        searchService = ignite.services().<SearchService>serviceProxy(SearchServiceImpl.SERVICE_NAME, SearchService.class, true);

        SecureRandom secureRandom = new SecureRandom();

        for (int i = 0; i < MAX_DOCUMENTS; i++) {

            StringBuilder text = new StringBuilder();

            for (int j = 0; j < 20 + secureRandom.nextInt(400); j++) {
                int index = secureRandom.nextInt(WORDS.length);
                String word = index < WORDS.length ? WORDS[index] : WORDS[0];

                text.append(word).append(" ");
            }

            searchService.addDocument(new DocumentImpl(i, text.toString()));
        }
    }

    @Test
    @Ignore
    public void testRebuildDocumentIndex() throws UnknownHostException {
        searchService.rebuildDocumentIndex();
    }

    @Test
    public void testSearch() throws UnknownHostException {
        List<SearchResult> searchResults = searchService.search("образом предсказывают эксперименты");

        Assert.assertNotNull(searchResults);
        Assert.assertFalse(searchResults.isEmpty());
    }

    @Test
    public void testGetRecommendation() throws Exception {
        List<SearchResult> searchResults = searchService.getRecommendation(2);

        Assert.assertNotNull(searchResults);
        Assert.assertFalse(searchResults.isEmpty());
    }

    @After
    public void tearDown() throws Exception {
        MetricsFactoryImpl.report();
    }
}