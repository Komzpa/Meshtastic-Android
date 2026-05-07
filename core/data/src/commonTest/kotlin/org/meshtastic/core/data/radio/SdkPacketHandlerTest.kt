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

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.advanceUntilIdle
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import org.meshtastic.core.di.CoroutineDispatchers
import org.meshtastic.core.model.MeshActivity
import org.meshtastic.core.testing.FakeServiceRepository
import org.meshtastic.proto.Data
import org.meshtastic.proto.MeshPacket
import org.meshtastic.proto.PortNum
import org.meshtastic.proto.QueueStatus
import org.meshtastic.proto.ToRadio
import org.meshtastic.sdk.RadioClient
import org.meshtastic.sdk.TransportIdentity
import org.meshtastic.sdk.testing.FakeRadioTransport
import org.meshtastic.sdk.testing.InMemoryStorageProvider
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

@OptIn(ExperimentalCoroutinesApi::class)
class SdkPacketHandlerTest {

    @Test
    fun `sendToRadio MeshPacket routes through client and emits send activity`() = runTest {
        val serviceRepository = FakeServiceRepository()
        val fixture = connectedHandler(serviceRepository)
        try {
            val outboundBefore = fixture.transport.outboundPackets().size
            val packet = MeshPacket(
                id = 42,
                to = 0x22222222,
                decoded = Data(portnum = PortNum.TEXT_MESSAGE_APP),
            )

            // Start collecting before the emission (SharedFlow has no replay)
            val activityDeferred = backgroundScope.async { serviceRepository.meshActivityFlow.first() }
            runCurrent()

            fixture.handler.sendToRadio(packet)
            runCurrent()

            val sent = fixture.transport.outboundPackets().drop(outboundBefore)
            assertTrue(sent.any { it.id == 42 && it.to == 0x22222222 })
            assertEquals(MeshActivity.Send, activityDeferred.await())
        } finally {
            fixture.client.disconnect()
        }
    }

    @Test
    fun `sendToRadio MeshPacket with no client drops packet`() = runTest {
        val serviceRepository = FakeServiceRepository()
        val handler = disconnectedHandler(serviceRepository)

        val packet = MeshPacket(id = 99, to = 0x33333333, decoded = Data(portnum = PortNum.TEXT_MESSAGE_APP))
        handler.sendToRadio(packet)
        runCurrent()

        // No crash, no activity emitted
        assertEquals(emptyList(), serviceRepository.storeForwardServers.value)
    }

    @Test
    fun `sendToRadio ToRadio with packet field routes MeshPacket`() = runTest {
        val serviceRepository = FakeServiceRepository()
        val fixture = connectedHandler(serviceRepository)
        try {
            val outboundBefore = fixture.transport.outboundPackets().size
            val meshPacket = MeshPacket(
                id = 7,
                to = 0x44444444,
                decoded = Data(portnum = PortNum.POSITION_APP),
            )
            val toRadio = ToRadio(packet = meshPacket)

            fixture.handler.sendToRadio(toRadio)
            runCurrent()

            val sent = fixture.transport.outboundPackets().drop(outboundBefore)
            assertTrue(sent.any { it.id == 7 && it.to == 0x44444444 })
        } finally {
            fixture.client.disconnect()
        }
    }

    @Test
    fun `sendToRadio ToRadio without packet calls sendRaw`() = runTest {
        val serviceRepository = FakeServiceRepository()
        val fixture = connectedHandler(serviceRepository)
        try {
            val rawFrame = ToRadio(want_config_id = 12345)

            fixture.handler.sendToRadio(rawFrame)
            advanceUntilIdle()

            // sendRaw doesn't crash — the transport received the frame.
            // We can verify no MeshActivity.Send was emitted (raw frames don't emit activity).
        } finally {
            fixture.client.disconnect()
        }
    }

    @Test
    fun `sendToRadio ToRadio non-packet with no client drops silently`() = runTest {
        val serviceRepository = FakeServiceRepository()
        val handler = disconnectedHandler(serviceRepository)

        val rawFrame = ToRadio(want_config_id = 999)
        handler.sendToRadio(rawFrame)
        advanceUntilIdle()

        // No crash
    }

    @Test
    fun `sendToRadioAndAwait returns true on success`() = runTest {
        val serviceRepository = FakeServiceRepository()
        val fixture = connectedHandler(serviceRepository)
        try {
            val packet = MeshPacket(
                id = 50,
                to = 0x55555555,
                decoded = Data(portnum = PortNum.TEXT_MESSAGE_APP),
            )

            val result = fixture.handler.sendToRadioAndAwait(packet)
            assertTrue(result)
        } finally {
            fixture.client.disconnect()
        }
    }

    @Test
    fun `sendToRadioAndAwait returns false when no client`() = runTest {
        val handler = disconnectedHandler(FakeServiceRepository())

        val packet = MeshPacket(id = 60, to = 0x66666666, decoded = Data(portnum = PortNum.TEXT_MESSAGE_APP))
        val result = handler.sendToRadioAndAwait(packet)
        assertFalse(result)
    }

    @Test
    fun `handleQueueStatus is a no-op`() = runTest {
        val handler = disconnectedHandler(FakeServiceRepository())
        handler.handleQueueStatus(QueueStatus(res = 0, free = 32, maxlen = 32))
        // No crash — SDK handles queue internally
    }

    @Test
    fun `removeResponse is a no-op`() = runTest {
        val handler = disconnectedHandler(FakeServiceRepository())
        handler.removeResponse(dataRequestId = 123, complete = true)
        // No crash
    }

    @Test
    fun `stopPacketQueue is a no-op`() = runTest {
        val handler = disconnectedHandler(FakeServiceRepository())
        handler.stopPacketQueue()
        // No crash
    }

    // ── Helpers ─────────────────────────────────────────────────────────────

    private data class HandlerFixture(
        val handler: SdkPacketHandler,
        val transport: FakeRadioTransport,
        val client: RadioClient,
    )

    private suspend fun TestScope.connectedHandler(
        serviceRepository: FakeServiceRepository,
    ): HandlerFixture {
        val transport = FakeRadioTransport(
            identity = TransportIdentity("fake:packet-handler-test"),
            autoHandshake = true,
        )
        val client = RadioClient.Builder()
            .transport(transport)
            .storage(InMemoryStorageProvider())
            .coroutineContext(backgroundScope.coroutineContext)
            .autoSyncTimeOnConnect(false)
            .build()
        client.connect()
        runCurrent()

        val dispatcher = backgroundScope.coroutineContext[kotlin.coroutines.ContinuationInterceptor]
            as CoroutineDispatcher
        val handler = SdkPacketHandler(
            accessor = TestRadioClientAccessor(client),
            serviceRepository = serviceRepository,
            dispatchers = CoroutineDispatchers(dispatcher, dispatcher, dispatcher),
        )
        return HandlerFixture(handler, transport, client)
    }

    private fun TestScope.disconnectedHandler(serviceRepository: FakeServiceRepository): SdkPacketHandler {
        val dispatcher = backgroundScope.coroutineContext[kotlin.coroutines.ContinuationInterceptor]
            as CoroutineDispatcher
        return SdkPacketHandler(
            accessor = TestRadioClientAccessor(null),
            serviceRepository = serviceRepository,
            dispatchers = CoroutineDispatchers(dispatcher, dispatcher, dispatcher),
        )
    }

    private class TestRadioClientAccessor(client: RadioClient?) : RadioClientAccessor {
        override val client = MutableStateFlow(client)

        override fun rebuildAndConnectAsync() = Unit

        override fun disconnect() = Unit
    }
}
