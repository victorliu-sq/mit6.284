package main

import "fmt"

var firstIdx int
var arr []int

func main() {
	arr = make([]int, 1)
	firstIdx = 0
	arr = append(arr, 1)
	arr = append(arr, 2)
	printArr(arr)

	// get subarray from idx to end
	arr2 := toEnd(1)
	printArr(arr2)

	// copy array: make a new arr with same length
	arr3 := make([]int, len(arr2))
	copy(arr3, arr2)
	printArr(arr3)
}

func printArr(arr []int) {
	format := fmt.Sprint(arr)
	fmt.Printf("%q\n", format)
}

func cutEnd(idx int) {
	arr = arr[0 : idx-firstIdx]
}

func toEnd(idx int) []int {
	return arr[idx:]
}
