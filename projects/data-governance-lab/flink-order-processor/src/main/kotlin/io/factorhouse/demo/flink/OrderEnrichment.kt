package io.factorhouse.demo.flink

import io.factorhouse.demo.model.EnrichedOrder
import io.factorhouse.demo.model.Order
import io.factorhouse.demo.model.User
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.util.Collector

class OrderEnrichment(
    private val userStateDescriptor: MapStateDescriptor<String, User>,
) : KeyedBroadcastProcessFunction<String, Order, User, EnrichedOrder>() {
    override fun processBroadcastElement(
        user: User,
        ctx: Context,
        out: Collector<EnrichedOrder>,
    ) {
        ctx.getBroadcastState(userStateDescriptor).put(user.userId, user)
    }

    override fun processElement(
        order: Order,
        ctx: ReadOnlyContext,
        out: Collector<EnrichedOrder?>,
    ) {
        val broadcastState = ctx.getBroadcastState(userStateDescriptor)
        val user = broadcastState.get(order.userId)

        val enrichedOrder =
            EnrichedOrder(
                orderId = order.orderId,
                productName = order.productName,
                category = order.category,
                quantity = order.quantity,
                unitPrice = order.unitPrice,
                createdAt = order.createdAt,
                userId = order.userId,
                userFirstName = user?.firstName,
                userLastName = user?.lastName,
                userEmail = user?.email,
                userPostalCode = user?.postalCode,
            )

        out.collect(enrichedOrder)
    }
}
