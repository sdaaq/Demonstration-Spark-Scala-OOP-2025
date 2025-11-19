package com.example
package constants

object ErrorMessage {
  val wrongSortingOrder: String => String = (order: String) => s"Order must be 'asc' or 'desc', got: $order"
  val failRename: (String, String) => String = (src: String, dst: String) => s"Failed to rename $src to $dst"
}
