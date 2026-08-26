import 'package:flutter/material.dart';
import 'package:http/http.dart' as http;
import 'dart:convert';

void main() {
  runApp(const AccountingApp());
}

class AccountingApp extends StatelessWidget {
  const AccountingApp({super.key});

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      debugShowCheckedModeBanner: false,
      title: 'تطبيق المحاسبة',
      theme: ThemeData.dark(),
      home: const DashboardScreen(),
    );
  }
}

class DashboardScreen extends StatefulWidget {
  const DashboardScreen({super.key});

  @override
  State<DashboardScreen> createState() => _DashboardScreenState();
}

class _DashboardScreenState extends State<DashboardScreen> {
  final String baseUrl = "https://Gold-trading-bot-production-7a0d.up.railway.app";
  
  double income = 0.0;
  double expense = 0.0;
  double netProfit = 0.0;
  List transactions = [];
  bool isLoading = true;

  @override
  void initState() {
    super.initState();
    fetchData();
  }

  Future<void> fetchData() async {
    setState(() => isLoading = true);
    try {
      final summaryRes = await http.get(Uri.parse('$baseUrl/api/summary'));
      final txRes = await http.get(Uri.parse('$baseUrl/api/transactions'));

      if (summaryRes.statusCode == 200 && txRes.statusCode == 200) {
        final summaryData = json.decode(summaryRes.body);
        final txData = json.decode(txRes.body);

        setState(() {
          income = (summaryData['total_income'] as num).toDouble();
          expense = (summaryData['total_expense'] as num).toDouble();
          netProfit = (summaryData['net_profit'] as num).toDouble();
          transactions = txData;
          isLoading = false;
        });
      }
    } catch (e) {
      setState(() => isLoading = false);
    }
  }

  Future<void> addTransaction(String title, double amount, String type) async {
    final response = await http.post(
      Uri.parse('$baseUrl/api/transactions'),
      headers: {'Content-Type': 'application/json'},
      body: json.encode({'title': title, 'amount': amount, 'type': type}),
    );

    if (response.statusCode == 200) {
      fetchData();
    }
  }

  void showAddDialog() {
    final titleController = TextEditingController();
    final amountController = TextEditingController();
    String selectedType = 'income';

    showDialog(
      context: context,
      builder: (context) => AlertDialog(
        title: const Text('إضافة عملية جديدة', textAlign: TextAlign.right),
        content: Column(
          mainAxisSize: MainAxisSize.min,
          children: [
            TextField(controller: titleController, decoration: const InputDecoration(labelText: 'الوصف')),
            TextField(controller: amountController, keyboardType: TextInputType.number, decoration: const InputDecoration(labelText: 'المبلغ')),
            DropdownButtonFormField<String>(
              value: selectedType,
              items: const [
                DropdownMenuItem(value: 'income', child: Text('إيراد / ربح')),
                DropdownMenuItem(value: 'expense', child: Text('مصروف / خسارة')),
              ],
              onChanged: (val) => selectedType = val!,
            ),
          ],
        ),
        actions: [
          TextButton(onPressed: () => Navigator.pop(context), child: const Text('إلغاء')),
          ElevatedButton(
            onPressed: () {
              if (titleController.text.isNotEmpty && amountController.text.isNotEmpty) {
                addTransaction(
                  titleController.text,
                  double.parse(amountController.text),
                  selectedType,
                );
                Navigator.pop(context);
              }
            },
            child: const Text('حفظ'),
          ),
        ],
      ),
    );
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(title: const Text('لوحة التحكم المحاسبية'), centerTitle: true),
      body: isLoading
          ? const Center(child: CircularProgressIndicator())
          : Padding(
              padding: const EdgeInsets.all(16.0),
              child: Column(
                children: [
                  Row(
                    children: [
                      _buildCard('الإيرادات', '\$$income', Colors.green),
                      const SizedBox(width: 8),
                      _buildCard('المصروفات', '\$$expense', Colors.red),
                      const SizedBox(width: 8),
                      _buildCard('الصافي', '\$$netProfit', netProfit >= 0 ? Colors.green : Colors.red),
                    ],
                  ),
                  const SizedBox(height: 20),
                  const Text('سجل العمليات', style: TextStyle(fontSize: 18, fontWeight: FontWeight.bold)),
                  const SizedBox(height: 10),
                  Expanded(
                    child: ListView.builder(
                      itemCount: transactions.length,
                      itemBuilder: (context, index) {
                        final tx = transactions[index];
                        final isIncome = tx['type'] == 'income';
                        return Card(
                          child: ListTile(
                            title: Text(tx['title']),
                            trailing: Text(
                              '\$${tx['amount']}',
                              style: TextStyle(
                                color: isIncome ? Colors.green : Colors.red,
                                fontWeight: FontWeight.bold,
                              ),
                            ),
                          ),
                        );
                      },
                    ),
                  ),
                ],
              ),
            ),
      floatingActionButton: FloatingActionButton(
        onPressed: showAddDialog,
        child: const Icon(Icons.add),
      ),
    );
  }

  Widget _buildCard(String title, String value, Color color) {
    return Expanded(
      child: Container(
        padding: const EdgeInsets.all(12),
        decoration: BoxDecoration(
          color: Colors.grey[900],
          borderRadius: BorderRadius.circular(10),
          border: Border.all(color: Colors.grey[800]!),
        ),
        child: Column(
          children: [
            Text(title, style: const TextStyle(fontSize: 12, color: Colors.grey)),
            const SizedBox(height: 5),
            Text(value, style: TextStyle(fontSize: 16, fontWeight: FontWeight.bold, color: color)),
          ],
        ),
      ),
    );
  }
}
