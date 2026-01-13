package io.factorhouse.demo.topology.processing

import io.factorhouse.demo.domain.model.EnrichedOrder
import io.factorhouse.demo.domain.model.Order
import io.factorhouse.demo.domain.model.User
import org.apache.flink.api.common.state.MapState
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.util.Collector
import org.apache.flink.util.OutputTag
import org.slf4j.LoggerFactory
import java.util.LinkedList

val MISSING_ORDERS_TAG = object : OutputTag<Order>("missing-orders") {}

class UserOrderJoinFunction : CoProcessFunction<Order, User, EnrichedOrder>() {
    private val logger = LoggerFactory.getLogger(UserOrderJoinFunction::class.java)
    private lateinit var userState: ValueState<User>
    private lateinit var pendingOrdersState: MapState<Long, MutableList<Order>>

    override fun open(parameters: Configuration) {
        userState =
            runtimeContext.getState(
                ValueStateDescriptor("user-state", User::class.java),
            )
        pendingOrdersState =
            runtimeContext.getMapState(
                MapStateDescriptor("pending-orders-map", Long::class.javaObjectType, MutableList::class.java as Class<MutableList<Order>>),
            )
    }

    override fun processElement1(
        order: Order,
        ctx: Context,
        out: Collector<EnrichedOrder>,
    ) {
        val user = userState.value()
        if (user != null) {
            out.collect(join(order, user))
        } else {
            // User missing. Buffer Order and set Timer based on processing time
            val current = ctx.timerService().currentProcessingTime()
            val timeoutTime = current + 30_000L // 30 Seconds

            logger.info("Order ${order.orderId} waiting for User. Timeout set for $timeoutTime")

            // Add to state
            var ordersAtTime = pendingOrdersState.get(timeoutTime)
            if (ordersAtTime == null) {
                ordersAtTime = LinkedList()
            }
            ordersAtTime.add(order)
            pendingOrdersState.put(timeoutTime, ordersAtTime)

            // Register Timer
            ctx.timerService().registerProcessingTimeTimer(timeoutTime)
        }
    }

    override fun processElement2(
        user: User,
        ctx: Context,
        out: Collector<EnrichedOrder>,
    ) {
        // Update User State
        userState.update(user)

        // Process ALL pending orders immediately
        val iterator = pendingOrdersState.iterator()
        while (iterator.hasNext()) {
            val entry = iterator.next()
            val orders = entry.value
            for (order in orders) {
                out.collect(join(order, user))
            }
            // Remove this timestamp entry from the map so the timer (when it fires) finds nothing
            iterator.remove()
        }
    }

    override fun onTimer(
        timestamp: Long,
        ctx: OnTimerContext,
        out: Collector<EnrichedOrder>,
    ) {
        // If the entry still exists, it means the User never arrived
        val orders = pendingOrdersState.get(timestamp)

        if (orders != null) {
            for (order in orders) {
                logger.warn("Timeout reached for Order ${order.orderId}. User not found. Sending to DLQ.")
                // Emit to Side Output
                ctx.output(MISSING_ORDERS_TAG, order)
            }
            // Clean up state
            pendingOrdersState.remove(timestamp)
        }
    }

    private fun join(
        order: Order,
        user: User,
    ): EnrichedOrder =
        EnrichedOrder(
            orderId = order.orderId,
            productName = order.productName,
            category = order.category,
            quantity = order.quantity,
            unitPrice = order.unitPrice,
            createdAt = order.createdAt,
            userId = order.userId,
            userFirstName = user.firstName,
            userLastName = user.lastName,
            userEmail = user.email,
            userPostalCode = user.postalCode,
        )
}
