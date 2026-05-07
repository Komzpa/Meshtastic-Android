/*
 * Copyright (c) 2026 Meshtastic LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.meshtastic.core.data.radio

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.test.runTest
import org.meshtastic.core.model.DeviceType
import org.meshtastic.core.model.InterfaceId
import org.meshtastic.core.testing.FakeRadioPrefs
import org.meshtastic.sdk.RadioClient
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

@OptIn(ExperimentalCoroutinesApi::class)
class SdkRadioInterfaceServiceTest {

    @Test
    fun `supportedDeviceTypes includes BLE TCP and USB`() {
        val service = createService()
        assertEquals(listOf(DeviceType.BLE, DeviceType.TCP, DeviceType.USB), service.supportedDeviceTypes)
    }

    @Test
    fun `currentDeviceAddressFlow delegates to radioPrefs devAddr`() {
        val prefs = FakeRadioPrefs()
        prefs.setDevAddr("x0123456789AB")
        val service = createService(prefs = prefs)

        assertEquals("x0123456789AB", service.currentDeviceAddressFlow.value)
    }

    @Test
    fun `getDeviceAddress returns current prefs value`() {
        val prefs = FakeRadioPrefs()
        prefs.setDevAddr("tTCP:192.168.1.1")
        val service = createService(prefs = prefs)

        assertEquals("tTCP:192.168.1.1", service.getDeviceAddress())
    }

    @Test
    fun `getDeviceAddress returns null when no address set`() {
        val service = createService()
        assertNull(service.getDeviceAddress())
    }

    @Test
    fun `setDeviceAddress stores new address and returns true`() {
        val prefs = FakeRadioPrefs()
        val service = createService(prefs = prefs)

        val changed = service.setDeviceAddress("x0123456789AB")

        assertTrue(changed)
        assertEquals("x0123456789AB", prefs.devAddr.value)
    }

    @Test
    fun `setDeviceAddress with same address returns false`() {
        val prefs = FakeRadioPrefs()
        prefs.setDevAddr("x0123456789AB")
        val service = createService(prefs = prefs)

        val changed = service.setDeviceAddress("x0123456789AB")
        assertFalse(changed)
    }

    @Test
    fun `setDeviceAddress to null clears address`() {
        val prefs = FakeRadioPrefs()
        prefs.setDevAddr("x0123456789AB")
        val service = createService(prefs = prefs)

        val changed = service.setDeviceAddress(null)

        assertTrue(changed)
        assertNull(prefs.devAddr.value)
    }

    @Test
    fun `isMockTransport returns true for mock address`() {
        val prefs = FakeRadioPrefs()
        prefs.setDevAddr("mMockDevice")
        val service = createService(prefs = prefs)

        assertTrue(service.isMockTransport())
    }

    @Test
    fun `isMockTransport returns false for BLE address`() {
        val prefs = FakeRadioPrefs()
        prefs.setDevAddr("x0123456789AB")
        val service = createService(prefs = prefs)

        assertFalse(service.isMockTransport())
    }

    @Test
    fun `isMockTransport returns false for TCP address`() {
        val prefs = FakeRadioPrefs()
        prefs.setDevAddr("tTCP:10.0.0.1")
        val service = createService(prefs = prefs)

        assertFalse(service.isMockTransport())
    }

    @Test
    fun `isMockTransport returns false when no address set`() {
        val service = createService()
        assertFalse(service.isMockTransport())
    }

    @Test
    fun `toInterfaceAddress composes interface id and rest`() {
        val service = createService()

        assertEquals("x0123456789AB", service.toInterfaceAddress(InterfaceId.BLUETOOTH, "0123456789AB"))
        assertEquals("tTCP:192.168.1.1", service.toInterfaceAddress(InterfaceId.TCP, "TCP:192.168.1.1"))
        assertEquals("s/dev/ttyUSB0", service.toInterfaceAddress(InterfaceId.SERIAL, "/dev/ttyUSB0"))
        assertEquals("mMock", service.toInterfaceAddress(InterfaceId.MOCK, "Mock"))
    }

    @Test
    fun `connect delegates to accessor rebuildAndConnectAsync`() {
        val accessor = RecordingAccessor()
        val service = createService(accessor = accessor)

        service.connect()

        assertTrue(accessor.rebuildCalled)
    }

    @Test
    fun `disconnect delegates to accessor disconnect`() = runTest {
        val accessor = RecordingAccessor()
        val service = createService(accessor = accessor)

        service.disconnect()

        assertTrue(accessor.disconnectCalled)
    }

    // ── Helpers ─────────────────────────────────────────────────────────────

    private fun createService(
        prefs: FakeRadioPrefs = FakeRadioPrefs(),
        accessor: RadioClientAccessor = RecordingAccessor(),
    ): SdkRadioInterfaceService = SdkRadioInterfaceService(prefs, accessor)

    private class RecordingAccessor : RadioClientAccessor {
        override val client = MutableStateFlow<RadioClient?>(null)

        var rebuildCalled = false
            private set

        var disconnectCalled = false
            private set

        override fun rebuildAndConnectAsync() {
            rebuildCalled = true
        }

        override fun disconnect() {
            disconnectCalled = true
        }
    }
}
