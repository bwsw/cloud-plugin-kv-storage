package com.bwsw.cloudstack.storage.kv.service;

import com.bwsw.cloudstack.storage.kv.entity.CreateStorageRequest;
import com.bwsw.cloudstack.storage.kv.entity.DeleteStorageRequest;
import com.bwsw.cloudstack.storage.kv.entity.KvStorage;
import com.bwsw.cloudstack.storage.kv.entity.ScrollableListResponse;
import com.bwsw.cloudstack.storage.kv.response.KvStorageResponse;
import com.cloud.exception.InvalidParameterValueException;
import com.cloud.user.AccountVO;
import com.cloud.user.dao.AccountDao;
import com.cloud.utils.db.SearchBuilder;
import com.cloud.utils.db.SearchCriteria;
import com.cloud.vm.VMInstanceVO;
import com.cloud.vm.dao.VMInstanceDao;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Strings;
import org.apache.cloudstack.api.ServerApiException;
import org.apache.cloudstack.api.response.ListResponse;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpStatus;
import org.apache.http.HttpVersion;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicStatusLine;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
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
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.intThat;
import static org.mockito.Matchers.longThat;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class KvStorageManagerImplTest {

    private static final Long ID = 1L;
    private static final String UUID = "61d12f36-0201-4035-b6fc-c7f768f583f1";
    private static final String STORAGE_UUID = "71d12f36-0201-4035-b6fc-c7f768f583f1";
    private static final String NAME = "test storage";
    private static final String DESCRIPTION = "test storage description";
    private static final Boolean HISTORY_ENABLED = true;
    private static final String UUID_PATTERN = "\\p{XDigit}{8}-\\p{XDigit}{4}-\\p{XDigit}{4}-\\p{XDigit}{4}-\\p{XDigit}{12}";
    private static final Integer TTL = 300000;
    private static final long TIMESTAMP = System.currentTimeMillis();
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
    private RestClient _restClient;

    @Mock
    private IndexRequest _indexRequest;

    @Mock
    private GetRequest _getRequest;

    @Mock
    private DeleteStorageRequest _deleteStorageRequest;

    @Mock
    private CreateStorageRequest _createStorageRequest;

    @Mock
    private UpdateRequest _updateRequest;

    @Mock
    private Response _response;

    @Mock
    private VMInstanceVO _vmInstanceVO;

    @Mock
    private SearchBuilder<VMInstanceVO> _searchBuilder;

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
        doThrow(new IOException()).when(_kvExecutor).create(_restHighLevelClient, _createStorageRequest);

        _kvStorageManager.createAccountStorage(ID, NAME, DESCRIPTION, HISTORY_ENABLED);
    }

    @Test
    public void testCreateAccountStorage() throws IOException {
        setAccountExpectations();
        setAccountRequestExpectations(UUID, NAME, DESCRIPTION, HISTORY_ENABLED);
        doNothing().when(_kvExecutor).create(_restHighLevelClient, _createStorageRequest);

        _kvStorageManager.createAccountStorage(ID, NAME, DESCRIPTION, HISTORY_ENABLED);

        verify(_kvExecutor).create(_restHighLevelClient, _createStorageRequest);
    }

    @Test
    public void testCreateAccountStorageDefaultHistorySettings() throws IOException {
        setAccountExpectations();
        setAccountRequestExpectations(UUID, NAME, DESCRIPTION, false);
        doNothing().when(_kvExecutor).create(_restHighLevelClient, _createStorageRequest);

        _kvStorageManager.createAccountStorage(ID, NAME, DESCRIPTION, null);

        verify(_kvExecutor).create(_restHighLevelClient, _createStorageRequest);
    }

    @Test
    public void testCreateVmStorageInvalidVm() {
        setExceptionExpectation(InvalidParameterValueException.class, "VM");

        when(_vmInstanceDao.findByUuid(UUID)).thenReturn(null);

        _kvStorageManager.createVmStorage(UUID);
    }

    @Test
    public void testCreateVmRequestException() throws IOException {
        setExceptionExpectation(ServerApiException.class, "storage");

        setVmExpectations();
        setVmRequestExpectations();
        doThrow(new IOException()).when(_kvExecutor).create(_restHighLevelClient, _createStorageRequest);

        _kvStorageManager.createVmStorage(UUID);
    }

    @Test
    public void testCreateVmStorage() throws IOException {
        setVmExpectations();
        setVmRequestExpectations();
        doNothing().when(_kvExecutor).create(_restHighLevelClient, _createStorageRequest);

        _kvStorageManager.createVmStorage(UUID);

        verify(_kvExecutor).create(_restHighLevelClient, _createStorageRequest);
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
    public void testCreateTempStorage() throws IOException {
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
                if (storage.getHistoryEnabled() == null || storage.getHistoryEnabled()) {
                    return false;
                }
                return true;
            }
        }))).thenReturn(_createStorageRequest);
        doNothing().when(_kvExecutor).create(_restHighLevelClient, _createStorageRequest);

        KvStorage result = _kvStorageManager.createTempStorage(TTL);
        assertNotNull(result);
        assertNotNull(result.getId());
        assertTrue(result.getId().matches(UUID_PATTERN));

        verify(_kvExecutor).create(_restHighLevelClient, _createStorageRequest);
    }

    @Test
    public void testListAccountStoragesNullPageSize() {
        setExceptionExpectation(InvalidParameterValueException.class, "page size");
        _kvStorageManager.listAccountStorages(ID, START_INDEX, null);
    }

    @Test
    public void testListAccountStoragesInvalidPageSize() {
        setExceptionExpectation(InvalidParameterValueException.class, "page size");
        _kvStorageManager.listAccountStorages(ID, START_INDEX, 0L);
    }

    @Test
    public void testListAccountStoragesNullStartIndex() throws IOException {
        testListAccountStorages(null, (int)DEFAULT_INDEX);
    }

    @Test
    public void testListAccountStoragesInvalidStartIndex() {
        setExceptionExpectation(InvalidParameterValueException.class, "start index");
        _kvStorageManager.listAccountStorages(ID, -1L, PAGE_SIZE);
    }

    @Test
    public void testListAccountStoragesInvalidAccount() {
        setExceptionExpectation(InvalidParameterValueException.class, "account");
        when(_accountDao.findById(ID)).thenReturn(null);

        _kvStorageManager.listAccountStorages(ID, START_INDEX, PAGE_SIZE);
    }

    @Test
    public void testListAccountStoragesRequestException() throws IOException {
        setExceptionExpectation(ServerApiException.class, "storage");

        setAccountExpectations();
        when(_kvRequestBuilder.getSearchRequest(UUID, (int)START_INDEX, (int)PAGE_SIZE)).thenReturn(_searchRequest);
        doThrow(new IOException()).when(_kvExecutor).search(_restHighLevelClient, _searchRequest, KvStorageResponse.class);

        _kvStorageManager.listAccountStorages(ID, START_INDEX, PAGE_SIZE);
    }

    @Test
    public void testListAccountStorages() throws IOException {
        testListAccountStorages(START_INDEX, (int)START_INDEX);
    }

    @Test
    public void testDeleteAccountStorageInvalidAccount() {
        setExceptionExpectation(InvalidParameterValueException.class, "account");
        when(_accountDao.findById(ID)).thenReturn(null);

        _kvStorageManager.deleteAccountStorage(ID, STORAGE_UUID);
    }

    @Test
    public void testDeleteAccountStorageNullStorageId() {
        setExceptionExpectation(InvalidParameterValueException.class, "storage");
        setAccountExpectations();

        _kvStorageManager.deleteAccountStorage(ID, null);
    }

    @Test
    public void testDeleteAccountStorageEmptyStorageId() {
        setExceptionExpectation(InvalidParameterValueException.class, "storage");
        setAccountExpectations();

        _kvStorageManager.deleteAccountStorage(ID, "");
    }

    @Test
    public void testDeleteAccountStorageGetRequestException() throws IOException {
        setAccountExpectations();
        when(_kvRequestBuilder.getGetRequest(STORAGE_UUID)).thenReturn(_getRequest);
        when(_kvExecutor.get(_restHighLevelClient, _getRequest, KvStorage.class)).thenThrow(new IOException());

        assertFalse(_kvStorageManager.deleteAccountStorage(ID, STORAGE_UUID));
    }

    @Test
    public void testDeleteAccountStorageNonexistentStorage() throws IOException {
        setExceptionExpectation(InvalidParameterValueException.class, "storage");
        setAccountExpectations();
        when(_kvRequestBuilder.getGetRequest(STORAGE_UUID)).thenReturn(_getRequest);
        when(_kvExecutor.get(_restHighLevelClient, _getRequest, KvStorage.class)).thenReturn(null);

        _kvStorageManager.deleteAccountStorage(ID, STORAGE_UUID);
    }

    @Test
    public void testDeleteAccountStorageInvalidStorageType() throws IOException {
        KvStorage storage = new KvStorage();
        storage.setType(KvStorage.KvStorageType.TEMP);

        setExceptionExpectation(InvalidParameterValueException.class, "type");
        setAccountExpectations();
        when(_kvRequestBuilder.getGetRequest(STORAGE_UUID)).thenReturn(_getRequest);
        when(_kvExecutor.get(_restHighLevelClient, _getRequest, KvStorage.class)).thenReturn(storage);

        _kvStorageManager.deleteAccountStorage(ID, STORAGE_UUID);
    }

    @Test
    public void testDeleteAccountStorageUnmatchedAccount() throws IOException {
        KvStorage storage = new KvStorage();
        storage.setType(KvStorage.KvStorageType.ACCOUNT);
        storage.setAccount("some");

        setExceptionExpectation(InvalidParameterValueException.class, "account");
        setAccountExpectations();
        when(_kvRequestBuilder.getGetRequest(STORAGE_UUID)).thenReturn(_getRequest);
        when(_kvExecutor.get(_restHighLevelClient, _getRequest, KvStorage.class)).thenReturn(storage);

        _kvStorageManager.deleteAccountStorage(ID, STORAGE_UUID);
    }

    @Test
    public void testDeleteAccountStorageDeleteRequestException() throws IOException {
        setAccountExpectations();
        setDeleteAccountStorageExpectations();
        when(_kvExecutor.delete(_restHighLevelClient, _deleteStorageRequest)).thenThrow(new IOException());

        assertFalse(_kvStorageManager.deleteAccountStorage(ID, STORAGE_UUID));
    }

    @Test
    public void testDeleteAccountStorage() throws IOException {
        setAccountExpectations();
        setDeleteAccountStorageExpectations();
        when(_kvExecutor.delete(_restHighLevelClient, _deleteStorageRequest)).thenReturn(true);

        assertTrue(_kvStorageManager.deleteAccountStorage(ID, STORAGE_UUID));
    }

    @Test
    public void testUpdateTempStorageNullStorageId() {
        setExceptionExpectation(InvalidParameterValueException.class, "id");

        _kvStorageManager.updateTempStorage(null, TTL);
    }

    @Test
    public void testUpdateTempStorageEmptyStorageId() {
        setExceptionExpectation(InvalidParameterValueException.class, "id");

        _kvStorageManager.updateTempStorage("", TTL);
    }

    @Test
    public void testUpdateTempStorageNullTtl() {
        testUpdateTempStorageInvalidTtl(null);
    }

    @Test
    public void testUpdateTempStorageNegativeTtl() {
        testUpdateTempStorageInvalidTtl(-1);
    }

    @Test
    public void testUpdateTempStorageZeroTtl() {
        testUpdateTempStorageInvalidTtl(0);
    }

    @Test
    public void testUpdateTempStorageBigTtl() {
        testUpdateTempStorageInvalidTtl(KvStorageManager.KvStorageMaxTtl.value() + 1);
    }

    @Test
    public void testUpdateTempStorageGetRequestException() throws IOException {
        setExceptionExpectation(ServerApiException.class, "storage");
        when(_kvRequestBuilder.getGetRequest(STORAGE_UUID)).thenReturn(_getRequest);
        when(_kvExecutor.get(_restHighLevelClient, _getRequest, KvStorage.class)).thenThrow(new IOException());

        _kvStorageManager.updateTempStorage(STORAGE_UUID, TTL);
    }

    @Test
    public void testUpdateTempStorageNonexistentStorage() throws IOException {
        setExceptionExpectation(InvalidParameterValueException.class, "storage");
        when(_kvRequestBuilder.getGetRequest(STORAGE_UUID)).thenReturn(_getRequest);
        when(_kvExecutor.get(_restHighLevelClient, _getRequest, KvStorage.class)).thenReturn(null);

        _kvStorageManager.updateTempStorage(STORAGE_UUID, TTL);
    }

    @Test
    public void testUpdateTempStorageInvalidStorageType() throws IOException {
        KvStorage storage = new KvStorage();
        storage.setType(KvStorage.KvStorageType.ACCOUNT);

        setExceptionExpectation(InvalidParameterValueException.class, "type");
        when(_kvRequestBuilder.getGetRequest(STORAGE_UUID)).thenReturn(_getRequest);
        when(_kvExecutor.get(_restHighLevelClient, _getRequest, KvStorage.class)).thenReturn(storage);

        _kvStorageManager.updateTempStorage(STORAGE_UUID, TTL);
    }

    @Test
    public void testUpdateTempStorageUpdateRequestException() throws IOException {
        setExceptionExpectation(ServerApiException.class, "storage");
        setUpdateTempStorageExpectations(TTL, TIMESTAMP);
        doThrow(new IOException()).when(_kvExecutor).update(_restHighLevelClient, _updateRequest);

        _kvStorageManager.updateTempStorage(STORAGE_UUID, TTL);
    }

    @Test
    public void testUpdateTempStorage() throws IOException {
        setUpdateTempStorageExpectations(TTL, TIMESTAMP);
        doNothing().when(_kvExecutor).update(_restHighLevelClient, _updateRequest);

        KvStorage result = _kvStorageManager.updateTempStorage(STORAGE_UUID, TTL);
        assertNotNull(result);
        assertEquals(STORAGE_UUID, result.getId());
        assertEquals(TTL, result.getTtl());
        assertEquals((Long)(TIMESTAMP + TTL), result.getExpirationTimestamp());
    }

    @Test
    public void testDeleteTempStorageNullStorageId() {
        setExceptionExpectation(InvalidParameterValueException.class, "storage");

        _kvStorageManager.deleteTempStorage(null);
    }

    @Test
    public void testDeleteTempStorageEmptyStorageId() {
        setExceptionExpectation(InvalidParameterValueException.class, "storage");

        _kvStorageManager.deleteTempStorage("");
    }

    @Test
    public void testDeleteTempStorageGetRequestException() throws IOException {
        when(_kvRequestBuilder.getGetRequest(STORAGE_UUID)).thenReturn(_getRequest);
        when(_kvExecutor.get(_restHighLevelClient, _getRequest, KvStorage.class)).thenThrow(new IOException());

        assertFalse(_kvStorageManager.deleteTempStorage(STORAGE_UUID));
    }

    @Test
    public void testDeleteTempStorageNonexistentStorage() throws IOException {
        setExceptionExpectation(InvalidParameterValueException.class, "storage");
        when(_kvRequestBuilder.getGetRequest(STORAGE_UUID)).thenReturn(_getRequest);
        when(_kvExecutor.get(_restHighLevelClient, _getRequest, KvStorage.class)).thenReturn(null);

        _kvStorageManager.deleteTempStorage(STORAGE_UUID);
    }

    @Test
    public void testDeleteTempStorageInvalidStorageType() throws IOException {
        KvStorage storage = new KvStorage();
        storage.setType(KvStorage.KvStorageType.ACCOUNT);

        setExceptionExpectation(InvalidParameterValueException.class, "type");
        when(_kvRequestBuilder.getGetRequest(STORAGE_UUID)).thenReturn(_getRequest);
        when(_kvExecutor.get(_restHighLevelClient, _getRequest, KvStorage.class)).thenReturn(storage);

        _kvStorageManager.deleteTempStorage(STORAGE_UUID);
    }

    @Test
    public void testDeleteTempStorageDeleteRequestException() throws IOException {
        setDeleteTempStorageExpectations();
        when(_kvExecutor.delete(_restHighLevelClient, _deleteStorageRequest)).thenThrow(new IOException());

        assertFalse(_kvStorageManager.deleteTempStorage(STORAGE_UUID));
    }

    @Test
    public void testDeleteTempStorage() throws IOException {
        setDeleteTempStorageExpectations();
        when(_kvExecutor.delete(_restHighLevelClient, _deleteStorageRequest)).thenReturn(true);

        assertTrue(_kvStorageManager.deleteTempStorage(STORAGE_UUID));
    }

    @Test
    public void testDeleteVmStorageInvalidVm() {
        setExceptionExpectation(InvalidParameterValueException.class, "VM");
        when(_vmInstanceDao.findByUuidIncludingRemoved(UUID)).thenReturn(null);

        _kvStorageManager.deleteVmStorage(UUID);
    }

    @Test
    public void testDeleteVmStorageGetRequestException() throws IOException {
        setVmExpectations();
        when(_kvRequestBuilder.getGetRequest(UUID)).thenReturn(_getRequest);
        when(_kvExecutor.get(_restHighLevelClient, _getRequest, KvStorage.class)).thenThrow(new IOException());

        assertFalse(_kvStorageManager.deleteVmStorage(UUID));
    }

    @Test
    public void testDeleteVmStorageNonexistentStorage() throws IOException {
        setExceptionExpectation(InvalidParameterValueException.class, "storage");
        setVmExpectations();
        when(_kvRequestBuilder.getGetRequest(UUID)).thenReturn(_getRequest);
        when(_kvExecutor.get(_restHighLevelClient, _getRequest, KvStorage.class)).thenReturn(null);

        _kvStorageManager.deleteVmStorage(UUID);
    }

    @Test
    public void testDeleteVmStorageInvalidStorageType() throws IOException {
        KvStorage storage = new KvStorage();
        storage.setType(KvStorage.KvStorageType.TEMP);

        setExceptionExpectation(InvalidParameterValueException.class, "type");
        setVmExpectations();
        when(_kvRequestBuilder.getGetRequest(UUID)).thenReturn(_getRequest);
        when(_kvExecutor.get(_restHighLevelClient, _getRequest, KvStorage.class)).thenReturn(storage);

        _kvStorageManager.deleteVmStorage(UUID);
    }

    @Test
    public void testDeleteVmStorageDeleteRequestException() throws IOException {
        setVmExpectations();
        setDeleteVmStorageExpectations();
        when(_kvExecutor.delete(_restHighLevelClient, _deleteStorageRequest)).thenThrow(new IOException());

        assertFalse(_kvStorageManager.deleteVmStorage(UUID));
    }

    @Test
    public void testDeleteVmStorage() throws IOException {
        setVmExpectations();
        setDeleteVmStorageExpectations();
        when(_kvExecutor.delete(_restHighLevelClient, _deleteStorageRequest)).thenReturn(true);

        assertTrue(_kvStorageManager.deleteVmStorage(UUID));
    }

    @Test
    public void testExpireTempStorages() throws IOException {
        Request request = new Request("POST", "http://localhost:9200", Collections.emptyMap(), new StringEntity("body"));
        when(_kvRequestBuilder.getExpireTempStorageRequest(longThat(greaterThanOrEqualTo(KvStorage.getCurrentTimestamp())))).thenReturn(request);
        when(_restHighLevelClient.getLowLevelClient()).thenReturn(_restClient);
        when(_restClient.performRequest(request.getMethod(), request.getEndpoint(), request.getParameters(), request.getEntity())).thenReturn(_response);
        when(_response.getStatusLine()).thenReturn(new BasicStatusLine(HttpVersion.HTTP_1_1, HttpStatus.SC_OK, null));

        _kvStorageManager.expireTempStorages();
    }

    @Test
    public void testCleanupStorages() throws IOException {
        when(_kvRequestBuilder.getDeletedStoragesRequest(intThat(greaterThan(0)), intThat(greaterThan(0)))).thenReturn(_searchRequest);
        ScrollableListResponse<KvStorage> response = new ScrollableListResponse<>("scrollId", Collections.singletonList(new KvStorage()));
        when(_kvExecutor.scroll(_restHighLevelClient, _searchRequest, KvStorage.class)).thenReturn(response);
        for (KvStorage storage : response.getResults()) {
            when(_kvRequestBuilder.getDeleteRequest(storage)).thenReturn(_deleteStorageRequest);
            when(_kvExecutor.delete(_restHighLevelClient, _deleteStorageRequest)).thenReturn(true);
        }
        SearchScrollRequest scrollRequest = new SearchScrollRequest();
        when(_kvRequestBuilder.getScrollRequest(eq(response.getScrollId()), intThat(greaterThan(0)))).thenReturn(scrollRequest);
        when(_kvExecutor.scroll(_restHighLevelClient, scrollRequest, KvStorage.class)).thenReturn(new ScrollableListResponse<>("id", null));

        _kvStorageManager.cleanupStorages();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testDeleteExpungedVmStorages() throws IOException {
        ScrollableListResponse<KvStorage> response = new ScrollableListResponse<>("scrollId", Collections.singletonList(new KvStorage()));
        List<VMInstanceVO> vmInstanceVOList = response.getResults().stream().map(storage -> {
            VMInstanceVO vm = mock(VMInstanceVO.class);
            when(vm.getUuid()).thenReturn(storage.getId());
            when(vm.isRemoved()).thenReturn(true);
            return vm;
        }).collect(Collectors.toList());
        SearchCriteria<VMInstanceVO> searchCriteria = mock(SearchCriteria.class);

        when(_kvRequestBuilder.getVmStoragesRequest(intThat(greaterThan(0)), intThat(greaterThan(0)))).thenReturn(_searchRequest);
        when(_kvExecutor.scroll(_restHighLevelClient, _searchRequest, KvStorage.class)).thenReturn(response);
        when(_searchBuilder.create()).thenReturn(searchCriteria);
        doNothing().when(searchCriteria).setParameters(anyString(), eq(response.getResults().stream().map(KvStorage::getId).toArray()));
        when(_vmInstanceDao.searchIncludingRemoved(same(searchCriteria), eq(null), eq(null), eq(false))).thenReturn(vmInstanceVOList);
        for (KvStorage storage : response.getResults()) {
            when(_kvRequestBuilder.getMarkDeletedRequest(storage)).thenReturn(_updateRequest);
            doNothing().when(_kvExecutor).update(_restHighLevelClient, _updateRequest);
        }
        SearchScrollRequest scrollRequest = new SearchScrollRequest();
        when(_kvRequestBuilder.getScrollRequest(eq(response.getScrollId()), intThat(greaterThan(0)))).thenReturn(scrollRequest);
        when(_kvExecutor.scroll(_restHighLevelClient, scrollRequest, KvStorage.class)).thenReturn(new ScrollableListResponse<>("id", null));

        _kvStorageManager.deleteExpungedVmStorages();
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

    private void testUpdateTempStorageInvalidTtl(Integer ttl) {
        setExceptionExpectation(InvalidParameterValueException.class, "TTL");

        _kvStorageManager.updateTempStorage(STORAGE_UUID, ttl);
    }

    private void testListAccountStorages(Long argStartIndex, int requestStartIndex) throws IOException {
        ListResponse<KvStorageResponse> expectedResponse = new ListResponse<>();

        setAccountExpectations();
        when(_kvRequestBuilder.getSearchRequest(UUID, requestStartIndex, (int)PAGE_SIZE)).thenReturn(_searchRequest);
        when(_kvExecutor.search(_restHighLevelClient, _searchRequest, KvStorageResponse.class)).thenReturn(expectedResponse);

        ListResponse<KvStorageResponse> response = _kvStorageManager.listAccountStorages(ID, argStartIndex, PAGE_SIZE);
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
        when(_vmInstanceDao.findByUuid(UUID)).thenReturn(_vmInstanceVO);
        when(_vmInstanceDao.findByUuidIncludingRemoved(UUID)).thenReturn(_vmInstanceVO);
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
        }))).thenReturn(_createStorageRequest);
    }

    private void setVmRequestExpectations() throws JsonProcessingException {
        when(_kvRequestBuilder.getCreateRequest(argThat(new CustomMatcher<KvStorage>("vm storage") {
            @Override
            public boolean matches(Object o) {
                if (!(o instanceof KvStorage)) {
                    return false;
                }
                KvStorage storage = (KvStorage)o;
                return UUID.equals(storage.getId()) && (storage.getHistoryEnabled() != null && !storage.getHistoryEnabled());
            }
        }))).thenReturn(_createStorageRequest);
    }

    private void setDeleteAccountStorageExpectations() throws IOException {
        KvStorage storage = new KvStorage();
        storage.setId(STORAGE_UUID);
        storage.setType(KvStorage.KvStorageType.ACCOUNT);
        storage.setAccount(UUID);

        setAccountExpectations();
        setDeleteStorageExpectations(storage);
    }

    private void setDeleteTempStorageExpectations() throws IOException {
        KvStorage storage = new KvStorage(STORAGE_UUID, TTL, TIMESTAMP);
        setDeleteStorageExpectations(storage);
    }

    private void setDeleteVmStorageExpectations() throws IOException {
        setDeleteStorageExpectations(new KvStorage(UUID, KvStorageManager.KvStorageVmHistoryEnabled.value()));
    }

    private void setDeleteStorageExpectations(KvStorage expected) throws IOException {
        when(_kvRequestBuilder.getGetRequest(expected.getId())).thenReturn(_getRequest);
        when(_kvExecutor.get(_restHighLevelClient, _getRequest, KvStorage.class)).thenReturn(expected);
        when(_kvRequestBuilder.getDeleteRequest(argThat(new CustomMatcher<KvStorage>("deleted storage") {
            @Override
            public boolean matches(Object o) {
                if (!(o instanceof KvStorage)) {
                    return false;
                }
                KvStorage storage = (KvStorage)o;
                return expected.getId().equals(storage.getId()) && storage.getDeleted() != null && storage.getDeleted();
            }
        }))).thenReturn(_deleteStorageRequest);
    }

    private void setUpdateTempStorageExpectations(Integer ttl, long creationTimestamp) throws IOException {
        KvStorage storage = new KvStorage();
        storage.setId(STORAGE_UUID);
        storage.setType(KvStorage.KvStorageType.TEMP);
        int oldTtl = TTL / 2;
        storage.setTtl(oldTtl);
        storage.setExpirationTimestamp(creationTimestamp + oldTtl);

        when(_kvRequestBuilder.getGetRequest(STORAGE_UUID)).thenReturn(_getRequest);
        when(_kvExecutor.get(_restHighLevelClient, _getRequest, KvStorage.class)).thenReturn(storage);
        when(_kvRequestBuilder.getUpdateTTLRequest(argThat(new CustomMatcher<KvStorage>("updated temp storage") {
            @Override
            public boolean matches(Object o) {
                if (!(o instanceof KvStorage)) {
                    return false;
                }
                KvStorage storage = (KvStorage)o;
                if (!STORAGE_UUID.equals(storage.getId())) {
                    return false;
                }
                if (storage.getTtl() == null || !storage.getTtl().equals(ttl)) {
                    return false;
                }
                if (storage.getExpirationTimestamp() == null || !storage.getExpirationTimestamp().equals(creationTimestamp + ttl)) {
                    return false;
                }
                return true;
            }
        }))).thenReturn(_updateRequest);
    }

    private void setExceptionExpectation(Class<? extends Exception> exceptionClass, String message) {
        expectedException.expect(exceptionClass);
        expectedException.expectMessage(message);
    }
}
