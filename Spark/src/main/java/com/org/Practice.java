package com.org;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class Practice {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Scanner scanner = new Scanner(System.in);
		long team = Long.parseLong(scanner.nextLine());

		while (team > 0) {
			long result = 0;
			long member = Long.parseLong(scanner.nextLine());
			String myTeam[] = scanner.nextLine().split(" ");
			String opponentTeam[] = scanner.nextLine().split(" ");

			// List<Long> list = new ArrayList<Long>();
			
			List<Long> list = Stream.of(myTeam).parallel().map(a->Long.parseLong(a)).sorted().collect(Collectors.toList());
			
			/*for (int i = 0; i < myTeam.length; i++) {
				list.add(Long.parseLong(myTeam[i]));
			}*/

			//Collections.sort(list);
			System.out.println(list);

			for (int i = 0; i < member; i++) {
				Long opponentPlayer = Long.parseLong(opponentTeam[i]);

				for (int j = 0; j < list.size(); j++) {
					Long teamPlayer = list.get(j);

					if (list.get(list.size() - 1) < opponentPlayer) {
						break;
					}

					if (teamPlayer > opponentPlayer) {
						list.remove(j);
						result++;
						break;
					}
				}
			}

			team--;
			System.out.println(result);
		}
	}
}
