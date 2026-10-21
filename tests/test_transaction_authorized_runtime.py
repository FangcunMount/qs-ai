"""Host admission delegates explicit decisions to public SDK owned operations only."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from qs_ai.bootstrap.transaction_messaging import TransactionAdmissionClosed, TransactionRuntime


@pytest.mark.parametrize("method", ["retry", "replan"])
async def test_authorized_entries_delegate_the_exact_host_decision_to_sdk_sender(method):
    driver = Mock()
    driver.retry, driver.replan = AsyncMock(), AsyncMock()
    driver.store = SimpleNamespace(retry=AsyncMock(), replan=AsyncMock())
    driver.stats.return_value = {"ready": True}
    runtime = TransactionRuntime(driver, admitted=True)
    authorization = object()
    result = await getattr(runtime, method)(authorization)
    getattr(driver, method).assert_awaited_once_with(authorization)
    assert result is getattr(driver, method).return_value
    driver.store.retry.assert_not_called()
    driver.store.replan.assert_not_called()


@pytest.mark.parametrize("method", ["retry", "replan"])
@pytest.mark.parametrize("stopping", [False, True])
async def test_pause_or_final_shutdown_rejects_authorized_advancement_before_sdk(method, stopping):
    driver = Mock()
    driver.retry, driver.replan = AsyncMock(), AsyncMock()
    driver.stats.return_value = {"ready": True}
    runtime = TransactionRuntime(driver, admitted=False)
    if stopping:
        runtime.stop_admission()
    with pytest.raises(TransactionAdmissionClosed):
        await getattr(runtime, method)(object())
    driver.retry.assert_not_called()
    driver.replan.assert_not_called()
    if not stopping:
        driver.stop_admission.assert_not_called()  # Checker remains alive during temporary pause.
