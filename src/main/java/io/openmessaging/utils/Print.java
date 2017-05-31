package io.openmessaging.utils;

public class Print {

	public static void print(String info) {

		System.out.println(info);
	}

	public static void print() {

		System.out.println();
	}

	public static void print(int[] numbers) {

		print(numbers, 0, numbers.length);
	}

	public static void print(int[] numbers, int p, int r) {
		System.out.print("[");
		for (int i = p; i < r; i++) {
			System.out.print(numbers[i] + " ");
		}
		System.out.println("]");
	}

	public static <T> void print(T[] array, int p, int r) {
		System.out.print("[");
		for (int i = p; i < r; i++) {
			System.out.print(array[i] + " ");
		}
		System.out.println("]");
	}

	public static void print(int[][] numbers) {
		// System.out.println(Arrays.deepToString(numbers));
		for (int[] t : numbers)
			print(t);
	}

	public static void print(byte[] bytes) {
		for (byte t : bytes)
			System.out.print(t);
		System.out.println();
	}

	public static <T> void print(Iterable<T> iterater) {
		for (T t : iterater) {
			System.out.print(t.toString() + " ");
		}
		System.out.println();
	}

	public static void print(Object o) {
		if (o == null) {
			System.out.println("Not found this Obj");
			return;
		}
		System.out.println(o.toString());
	}

	public static void main(String[] args) {

	}
}
