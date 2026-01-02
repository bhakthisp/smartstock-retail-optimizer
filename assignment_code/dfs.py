# DFS using adjacency matrix

def dfs(matrix, start, visited):
    visited[start] = True
    print(start, end=' ')
    
    for i in range(len(matrix)):
        if matrix[start][i] == 1 and not visited[i]:
            dfs(matrix, i, visited)

# Input number of vertices
n = int(input("Enter number of vertices: "))

# Input adjacency matrix
print("Enter adjacency matrix row by row (0 or 1):")
adj_matrix = []
for i in range(n):
    row = list(map(int, input(f"Row {i}: ").split()))
    if len(row) != n:
        print("Invalid row length! Please enter exactly", n, "values.")
        exit()
    adj_matrix.append(row)

# Input starting node (as index 0 to n-1)
start_node = int(input(f"Enter starting node (0 to {n-1}): "))

visited = [False] * n
print("DFS traversal:")
dfs(adj_matrix, start_node, visited)
