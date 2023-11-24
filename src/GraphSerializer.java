import java.util.*;

public class GraphSerializer {

    private HashMap<Integer, ArrayList<Triple>> subjectIndex;
    private HashMap<Integer, ArrayList<Triple>> objectIndex;
    private Set<Integer> visited = new HashSet<>();
    public ArrayList<Triple> orderedTriples = new ArrayList<>();

    public GraphSerializer(HashMap<Integer, ArrayList<Triple>> subjectIndex){
        this.subjectIndex = subjectIndex;
        //this.objectIndex = objectIndex;
    }

    // The DFS function
    private void dfs(Integer startSubject) {
        Stack<Integer> stack = new Stack<>();
        stack.push(startSubject);

        while (!stack.isEmpty()) {
            Integer currentSubject = stack.pop();
            if (!visited.contains(currentSubject)) {
                visited.add(currentSubject);
                List<Triple> triples = subjectIndex.getOrDefault(currentSubject, new ArrayList<>());

                for (Triple triple : triples) {
                    int obj = triple.triples[2];
                    if (!visited.contains(obj)) {
                        orderedTriples.add(triple);
                        stack.push(obj);
                    }
                }
            }
        }
    }

    public ArrayList<Triple> dfsAll(Set<Integer> vertices) {
        for (int vertex : vertices) {
            if (!visited.contains(vertex)) {
                dfs(vertex);
            }
        }
        return orderedTriples;
    }
    // BFS
    private void bfs(Integer startSubject) {
        Queue<Integer> queue = new LinkedList<>();
        queue.offer(startSubject);

        while (!queue.isEmpty()) {
            Integer currentSubject = queue.poll();
            if (!visited.contains(currentSubject)) {
                visited.add(currentSubject);
                List<Triple> triples = subjectIndex.getOrDefault(currentSubject, new ArrayList<>());

                for (Triple triple : triples) {
                    int obj = triple.triples[2];
                    if (!visited.contains(obj)) {
                        orderedTriples.add(triple);
                        queue.offer(obj);
                    }
                }
            }
        }
    }

    public ArrayList<Triple> bfsAll(Set<Integer> vertices) {
        for (int vertex : vertices) {
            if (!visited.contains(vertex)) {
                bfs(vertex);
            }
        }
        return orderedTriples;
    }
}
