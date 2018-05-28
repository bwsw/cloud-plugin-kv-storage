package com.bwsw.cloudstack.storage.kv.service;

import com.bwsw.cloudstack.storage.kv.entity.KvStorage;
import com.cloud.exception.InvalidParameterValueException;
import com.cloud.user.AccountVO;
import com.cloud.user.dao.AccountDao;
import com.cloud.vm.VMInstanceVO;
import com.cloud.vm.dao.VMInstanceDao;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Strings;
import org.apache.cloudstack.api.ServerApiException;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.index.IndexRequest;
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
    private static final String UUID_PATTERN = "\\p{XDigit}{8}-\\p{XDigit}{4}-\\p{XDigit}{4}-\\p{XDigit}{4}-\\p{XDigit}{12}";

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

    @Test
    public void testCreateAccountStorageInvalidAccount() {
        expectedException.expect(InvalidParameterValueException.class);
        expectedException.expectMessage("account");
        when(_accountDao.findById(ID)).thenReturn(null);

        _kvStorageManager.createAccountStorage(ID, NAME, DESCRIPTION);
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
        expectedException.expect(InvalidParameterValueException.class);
        expectedException.expectMessage("description");

        setAccountExpectations();

        _kvStorageManager.createAccountStorage(ID, NAME, StringUtils.repeat("A", KvStorageManager.KvStorageMaxDescriptionLength.value() + 1));
    }

    @Test
    public void testCreateAccountStorageRequestException() throws IOException {
        expectedException.expect(ServerApiException.class);
        expectedException.expectMessage("storage");

        setAccountExpectations();
        setAccountRequestExpectations();
        doThrow(new IOException()).when(_kvExecutor).index(_restHighLevelClient, _indexRequest);

        _kvStorageManager.createAccountStorage(ID, NAME, DESCRIPTION);
    }

    @Test
    public void testCreateAccountStorage() throws IOException {
        setAccountExpectations();
        setAccountRequestExpectations();
        doNothing().when(_kvExecutor).index(_restHighLevelClient, _indexRequest);

        _kvStorageManager.createAccountStorage(ID, NAME, DESCRIPTION);

        verify(_kvExecutor).index(_restHighLevelClient, _indexRequest);
    }

    @Test
    public void testCreateVmStorageInvalidVm() {
        expectedException.expect(InvalidParameterValueException.class);
        expectedException.expectMessage("virtual machine");
        when(_vmInstanceDao.findById(ID)).thenReturn(null);

        _kvStorageManager.createVmStorage(ID);
    }

    @Test
    public void testCreateVmRequestException() throws IOException {
        expectedException.expect(ServerApiException.class);
        expectedException.expectMessage("storage");

        setVmExpectations();
        setVmRequestExpectations();
        doThrow(new IOException()).when(_kvExecutor).index(_restHighLevelClient, _indexRequest);

        _kvStorageManager.createVmStorage(ID);
    }

    @Test
    public void testCreateVm() throws IOException {
        setVmExpectations();
        setVmRequestExpectations();
        doNothing().when(_kvExecutor).index(_restHighLevelClient, _indexRequest);

        _kvStorageManager.createVmStorage(ID);

        verify(_kvExecutor).index(_restHighLevelClient, _indexRequest);
    }

    private void testCreateAccountStorageInvalidName(String name) {
        expectedException.expect(InvalidParameterValueException.class);
        expectedException.expectMessage("name");

        setAccountExpectations();

        _kvStorageManager.createAccountStorage(ID, name, DESCRIPTION);
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

    private void setAccountRequestExpectations() throws JsonProcessingException {
        when(_kvRequestBuilder.getCreateRequest(argThat(new CustomMatcher<KvStorage>("account storage") {
            @Override
            public boolean matches(Object o) {
                if (!(o instanceof KvStorage)) {
                    return false;
                }
                KvStorage storage = (KvStorage)o;
                if (Strings.isNullOrEmpty(storage.getId()) || !storage.getId().matches(UUID_PATTERN) || storage.getId().equals(UUID)) {
                    return false;
                }
                if (!UUID.equals(storage.getAccount())) {
                    return false;
                }
                if (!NAME.equals(storage.getName())) {
                    return false;
                }
                if (!DESCRIPTION.equals(storage.getDescription())) {
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
                return UUID.equals(storage.getId());
            }
        }))).thenReturn(_indexRequest);
    }
}
