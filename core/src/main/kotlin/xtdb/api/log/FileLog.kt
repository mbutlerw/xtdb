package xtdb.api.log

import xtdb.api.log.FileLog.Subscription
import java.util.concurrent.CompletableFuture

interface FileLog {
    fun appendFileNotification(notification: Notification): CompletableFuture<Unit>
    fun subscribeFileNotifications(subscriber: Subscriber): Subscription

    interface Notification
    fun interface Subscription : AutoCloseable

    interface Subscriber {
        fun accept(record: Notification)
    }

    companion object {
        @JvmField
        val SOLO = object : FileLog {
            private var sub: Subscriber? = null

            override fun appendFileNotification(notification: Notification): CompletableFuture<Unit> {
                sub?.accept(notification)
                return CompletableFuture.completedFuture(Unit)
            }

            override fun subscribeFileNotifications(subscriber: Subscriber): Subscription {
                sub = subscriber
                return Subscription { sub = null }
            }
        }
    }
}
