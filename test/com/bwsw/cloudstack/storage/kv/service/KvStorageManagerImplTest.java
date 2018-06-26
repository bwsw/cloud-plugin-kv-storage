package com.bwsw.cloudstack.storage.kv.service;

import com.bwsw.cloudstack.storage.kv.entity.KvStorage;
import com.bwsw.cloudstack.storage.kv.response.KvStorageResponse;
import com.cloud.exception.InvalidParameterValueException;
import com.cloud.user.AccountVO;
import com.cloud.user.dao.AccountDao;
import com.cloud.vm.VMInstanceVO;
import com.cloud.vm.dao.VMInstanceDao;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Strings;
import org.apache.cloudstack.api.ServerApiException;
import org.apache.cloudstack.api.response.ListResponse;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.RestHighLevelClient;
import org.hamcrest.CustomMatcher;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.time.Instant;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class KvStorageManagerImplTest {

    private static final Long ID = 1L;
    private static final String UUID = "61d12f36-0201-4035-b6fc-c7f768f583f1";
    private static final String NAME = "test storage";
    private static final String DESCRIPTION = "test storage description";
    private static final Boolean HISTORY_ENABLED = true;
    private static final String UUID_PATTERN = "\\p{XDigit}{8}-\\p{XDigit}{4}-\\p{XDigit}{4}-\\p{XDigit}{4}-\\p{XDigit}{12}";
    private static final Integer TTL = 300000;
    private static final long PAGE_SIZE = 5L;
    private static final long START_INDEX = 10L;
    private static final long DEFAULT_INDEX = 0L;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Mock
    private AccountDao _accountDao;

    @Mock
    private VMInstanceDao _vmInstanceDao;

    @Mock
    private KvRequestBuilder _kvRequestBuilder;

    @Mock
    private KvExecutor _kvExecutor;

    @Mock
    private RestHighLevelClient _restHighLevelClient;

    @Mock
    private IndexRequest _indexRequest;

    @Mock
    private VMInstanceVO _vmInstanceVO;

    @InjectMocks
    private KvStorageManagerImpl _kvStorageManager = new KvStorageManagerImpl();

    private SearchRequest _searchRequest = new SearchRequest();

    @Test
    public void testCreateAccountStorageInvalidAccount() {
        setExceptionExpectation(InvalidParameterValueException.class, "account");

        when(_accountDao.findById(ID)).thenReturn(null);

        _kvStorageManager.createAccountStorage(ID, NAME, DESCRIPTION, HISTORY_ENABLED);
    }

    @Test
    public void testCreateAccountStorageNullName() {
        testCreateAccountStorageInvalidName(null);
    }

    @Test
    public void testCreateAccountStorageEmptyName() {
        testCreateAccountStorageInvalidName("");
    }

    @Test
    public void testCreateAccountStorageLongName() {
        testCreateAccountStorageInvalidName(StringUtils.repeat("A", KvStorageManager.KvStorageMaxNameLength.value() + 1));
    }

    @Test
    public void testCreateAccountStorageLongDescription() {
        setExceptionExpectation(InvalidParameterValueException.class, "description");

        setAccountExpectations();

        _kvStorageManager.createAccountStorage(ID, NAME, StringUtils.repeat("A", KvStorageManager.KvStorageMaxDescriptionLength.value() + 1), HISTORY_ENABLED);
    }

    @Test
    public void testCreateAccountStorageRequestException() throws IOException {
        setExceptionExpectation(ServerApiException.class, "storage");

        setAccountExpectations();
        setAccountRequestExpectations(UUID, NAME, DESCRIPTION, HISTORY_ENABLED);
        doThrow(new IOException()).when(_kvExecutor).index(_restHighLevelClient, _indexRequest);

        _kvStorageManager.createAccountStorage(ID, NAME, DESCRIPTION, HISTORY_ENABLED);
    }

    @Test
    public void testCreateAccountStorage() throws IOException {
        setAccountExpectations();
        setAccountRequestExpectations(UUID, NAME, DESCRIPTION, HISTORY_ENABLED);
        doNothing().when(_kvExecutor).index(_restHighLevelClient, _indexRequest);

        _kvStorageManager.createAccountStorage(ID, NAME, DESCRIPTION, HISTORY_ENABLED);

        verify(_kvExecutor).index(_restHighLevelClient, _indexRequest);
    }

    @Test
    public void testCreateAccountStorageDefaultHistorySettings() throws IOException {
        setAccountExpectations();
        setAccountRequestExpectations(UUID, NAME, DESCRIPTION, false);
        doNothing().when(_kvExecutor).index(_restHighLevelClient, _indexRequest);

        _kvStorageManager.createAccountStorage(ID, NAME, DESCRIPTION, null);

        verify(_kvExecutor).index(_restHighLevelClient, _indexRequest);
    }

    @Test
    public void testCreateVmStorageInvalidVm() {
        setExceptionExpectation(InvalidParameterValueException.class, "virtual machine");

        when(_vmInstanceDao.findById(ID)).thenReturn(null);

        _kvStorageManager.createVmStorage(ID, HISTORY_ENABLED);
    }

    @Test
    public void testCreateVmRequestException() throws IOException {
        setExceptionExpectation(ServerApiException.class, "storage");

        setVmExpectations();
        setVmRequestExpectations();
        doThrow(new IOException()).when(_kvExecutor).index(_restHighLevelClient, _indexRequest);

        _kvStorageManager.createVmStorage(ID, HISTORY_ENABLED);
    }

    @Test
    public void testCreateVm() throws IOException {
        setVmExpectations();
        setVmRequestExpectations();
        doNothing().when(_kvExecutor).index(_restHighLevelClient, _indexRequest);

        _kvStorageManager.createVmStorage(ID, HISTORY_ENABLED);

        verify(_kvExecutor).index(_restHighLevelClient, _indexRequest);
    }

    @Test
    public void testCreateTempStorageNullTtl() {
        testCreateTempStorageInvalidTtl(null);
    }

    @Test
    public void testCreateTempStorageNegativeTtl() {
        testCreateTempStorageInvalidTtl(-1);
    }

    @Test
    public void testCreateTempStorageZeroTtl() {
        testCreateTempStorageInvalidTtl(0);
    }

    @Test
    public void testCreateTempStorageBigTtl() {
        testCreateTempStorageInvalidTtl(KvStorageManager.KvStorageMaxTtl.value() + 1);
    }

    @Test
    public void testCreateTempStorage() throws JsonProcessingException {
        when(_kvRequestBuilder.getCreateRequest(argThat(new CustomMatcher<KvStorage>("temp storage") {
            @Override
            public boolean matches(Object o) {
                if (!(o instanceof KvStorage)) {
                    return false;
                }
                KvStorage storage = (KvStorage)o;
                if (Strings.isNullOrEmpty(storage.getId()) || !storage.getId().matches(UUID_PATTERN)) {
                    return false;
                }
                if (!TTL.equals(storage.getTtl())) {
                    return false;
                }
                if (storage.getExpirationTimestamp() == null || storage.getExpirationTimestamp() - storage.getTtl() > Instant.now().toEpochMilli()) {
                    return false;
                }
                return true;
            }
        }))).thenReturn(_indexRequest);

        _kvStorageManager.createTempStorage(TTL);
    }

    @Test
    public void testListStoragesNullPageSize() {
        setExceptionExpectation(InvalidParameterValueException.class, "page size");
        _kvStorageManager.listStorages(ID, START_INDEX, null);
    }

    @Test
    public void testListStoragesInvalidPageSize() {
        setExceptionExpectation(InvalidParameterValueException.class, "page size");
        _kvStorageManager.listStorages(ID, START_INDEX, 0L);
    }

    @Test
    public void testListStoragesNullStartIndex() throws IOException {
        testListStorages(null, (int)DEFAULT_INDEX);
    }

    @Test
    public void testListStoragesInvalidStartIndex() {
        setExceptionExpectation(InvalidParameterValueException.class, "start index");
        _kvStorageManager.listStorages(ID, -1L, PAGE_SIZE);
    }

    @Test
    public void testListStoragesInvalidAccount() {
        setExceptionExpectation(InvalidParameterValueException.class, "account");
        when(_accountDao.findById(ID)).thenReturn(null);

        _kvStorageManager.listStorages(ID, START_INDEX, PAGE_SIZE);
    }

    @Test
    public void testListStoragesRequestException() throws IOException {
        setExceptionExpectation(ServerApiException.class, "storage");

        setAccountExpectations();
        when(_kvRequestBuilder.getSearchRequest(UUID, (int)START_INDEX, (int)PAGE_SIZE)).thenReturn(_searchRequest);
        doThrow(new IOException()).when(_kvExecutor).search(_restHighLevelClient, _searchRequest, KvStorageResponse.class);

        _kvStorageManager.listStorages(ID, START_INDEX, PAGE_SIZE);
    }

    @Test
    public void testListStorages() throws IOException {
        testListStorages(START_INDEX, (int)START_INDEX);
    }

    private void testCreateAccountStorageInvalidName(String name) {
        setExceptionExpectation(InvalidParameterValueException.class, "name");

        setAccountExpectations();

        _kvStorageManager.createAccountStorage(ID, name, DESCRIPTION, HISTORY_ENABLED);
    }

    private void testCreateTempStorageInvalidTtl(Integer ttl) {
        setExceptionExpectation(InvalidParameterValueException.class, "TTL");

        _kvStorageManager.createTempStorage(ttl);
    }

    private void testListStorages(Long argStartIndex, int requestStartIndex) throws IOException {
        ListResponse<KvStorageResponse> expectedResponse = new ListResponse<>();

        setAccountExpectations();
        when(_kvRequestBuilder.getSearchRequest(UUID, requestStartIndex, (int)PAGE_SIZE)).thenReturn(_searchRequest);
        when(_kvExecutor.search(_restHighLevelClient, _searchRequest, KvStorageResponse.class)).thenReturn(expectedResponse);

        ListResponse<KvStorageResponse> response = _kvStorageManager.listStorages(ID, argStartIndex, PAGE_SIZE);
        assertEquals(expectedResponse, response);
    }

    private void setAccountExpectations() {
        AccountVO accountVO = new AccountVO();
        accountVO.setId(ID);
        accountVO.setUuid(UUID);

        when(_accountDao.findById(ID)).thenReturn(accountVO);
    }

    private void setVmExpectations() {
        when(_vmInstanceVO.getUuid()).thenReturn(UUID);
        when(_vmInstanceDao.findById(ID)).thenReturn(_vmInstanceVO);
    }

    private void setAccountRequestExpectations(String uuid, String name, String description, Boolean historyEnabled) throws JsonProcessingException {
        when(_kvRequestBuilder.getCreateRequest(argThat(new CustomMatcher<KvStorage>("account storage") {
            @Override
            public boolean matches(Object o) {
                if (!(o instanceof KvStorage)) {
                    return false;
                }
                KvStorage storage = (KvStorage)o;
                if (Strings.isNullOrEmpty(storage.getId()) || !storage.getId().matches(UUID_PATTERN) || storage.getId().equals(uuid)) {
                    return false;
                }
                if (!uuid.equals(storage.getAccount())) {
                    return false;
                }
                if (!name.equals(storage.getName())) {
                    return false;
                }
                if (!description.equals(storage.getDescription())) {
                    return false;
                }
                if (!historyEnabled.equals(storage.getHistoryEnabled())) {
                    return false;
                }
                return true;
            }
        }))).thenReturn(_indexRequest);
    }

    private void setVmRequestExpectations() throws JsonProcessingException {
        when(_kvRequestBuilder.getCreateRequest(argThat(new CustomMatcher<KvStorage>("vm storage") {
            @Override
            public boolean matches(Object o) {
                if (!(o instanceof KvStorage)) {
                    return false;
                }
                KvStorage storage = (KvStorage)o;
                return UUID.equals(storage.getId()) && HISTORY_ENABLED.equals(storage.getHistoryEnabled());
            }
        }))).thenReturn(_indexRequest);
    }

    private void setExceptionExpectation(Class<? extends Exception> exceptionClass, String message) {
        expectedException.expect(exceptionClass);
        expectedException.expectMessage(message);
    }
}
